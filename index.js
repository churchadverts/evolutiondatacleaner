import { WebSocket } from 'ws';
globalThis.WebSocket = WebSocket;

import express from 'express';
import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';

dotenv.config();

const app = express();
app.use(express.json({ limit: '50mb' }));

const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_SERVICE_KEY,
    { auth: { persistSession: false } }
);

const EVOLUTION_URL          = process.env.EVOLUTION_URL          || 'http://localhost:8080';
const EVOLUTION_API_KEY      = process.env.EVOLUTION_API_KEY      || '';
const INSTRUCTIONS_URL       = process.env.INSTRUCTIONS_URL       || 'http://localhost:8002';  // system1 / persona pipeline
const PRODUCTS_HANDLER_URL   = process.env.PRODUCTS_HANDLER_URL   || 'http://localhost:3002';  // image harvest + vision
const LEADS_ENRICHMENT_URL   = process.env.LEADS_ENRICHMENT_URL   || 'http://localhost:3003';  // lead data enrichment
const ENRICHMENT_WORKER_URL  = process.env.ENRICHMENT_WORKER_URL  || 'http://localhost:3001';  // existing status-update worker
const HISTORY_DAYS           = parseInt(process.env.HISTORY_DAYS  || '90');

// Auto-activation constants — overridable via billing config
const AUTO_ACTIVATE_LEADS   = 30;
const AUTO_APPROVE_PRODUCTS = 30;

// ============================================================
// SECTION 1 — BILLING CONFIG
// ============================================================
// Keys consumed from followup_billing_config:
//   lead_ingestion_min_kes     — KES per activated lead (default 0.75)
//   usd_to_kes_rate            — exchange rate          (default 130)
//
// Note: bot_build_cost_multiplier and bot_build_min_balance_usd
// have been moved to instructions-generator — they only apply there.
// ============================================================

let _billingConfig   = null;
let _billingConfigAt = 0;

async function getBillingConfig() {
    if (_billingConfig && (Date.now() - _billingConfigAt) < 5 * 60 * 1000) return _billingConfig;
    try {
        const { data } = await supabase.from('followup_billing_config').select('key, value');
        if (data) {
            _billingConfig   = data.reduce((acc, r) => { acc[r.key] = r.value; return acc; }, {});
            _billingConfigAt = Date.now();
        }
    } catch (e) {
        console.error('[Billing] Config fetch failed:', e.message);
        _billingConfig = _billingConfig || {};
    }
    return _billingConfig || {};
}

async function getBillingValue(key, defaultValue) {
    const config = await getBillingConfig();
    const val    = config[key];
    if (val === undefined || val === null) return defaultValue;
    const num = parseFloat(val);
    return isNaN(num) ? defaultValue : num;
}

/**
 * Deduct KES from a business balance.
 * Converts KES → USD using the config rate, writes to business_balances.
 * Returns { ok, reason?, charged_kes?, charged_usd?, new_balance_usd? }
 *
 * This is the ONLY place in the cleaner that touches money.
 * Top-ups go through the billing edge function — never here.
 */
async function chargeKes(businessId, amountKes, description) {
    try {
        const rate      = await getBillingValue('usd_to_kes_rate', 130);
        const amountUsd = amountKes / rate;

        const { data: balance } = await supabase
            .from('business_balances')
            .select('balance_usd')
            .eq('business_id', businessId)
            .single();

        if (!balance) return { ok: false, reason: 'no_balance_row' };

        const newBalance = parseFloat(balance.balance_usd) - amountUsd;
        if (newBalance < 0) return { ok: false, reason: 'insufficient_funds', available: balance.balance_usd };

        const { error } = await supabase
            .from('business_balances')
            .update({ balance_usd: newBalance, updated_at: new Date().toISOString() })
            .eq('business_id', businessId);

        if (error) throw error;

        console.log(`  [Billing] ✓ Charged ${amountKes.toFixed(2)} KES ($${amountUsd.toFixed(4)}) — ${description}`);
        return { ok: true, charged_kes: amountKes, charged_usd: amountUsd, new_balance_usd: newBalance };
    } catch (e) {
        console.error(`  [Billing] chargeKes error: ${e.message}`);
        return { ok: false, reason: e.message };
    }
}

// ============================================================
// SECTION 2 — ONBOARDING PROGRESS WRITER
// ============================================================
// business_onboarding is the single source of truth the frontend polls.
// This service owns ALL writes to sync_status.stage.
// Workers write only their own count columns (e.g. products_auto_approved_count)
// via direct Supabase calls — they never write sync_status.stage.
//
// Stage ownership:
//   history_received           ← processHistorySync (phase 1)
//   awaiting_lead_activation   ← processHistorySync (phase 1, after pre-scan)
//   auto_activating            ← autoActivateLeads
//   insufficient_funds_auto    ← autoActivateLeads (charge failed)
//   leads_activated            ← autoActivateLeads (success) / /leads/activate
//   bot_building               ← triggerInstructionsGenerator
//   bot_build_failed           ← triggerInstructionsGenerator (on error)
//   products_auto_approved     ← stageAndAutoApproveProductImages (first run)
//   products_discovered        ← stageAndAutoApproveProductImages (pending only)
//   products_approved          ← /products/approve-discovery
//   connected                  ← processConnectionUpdate
//   disconnected               ← processConnectionUpdate
// ============================================================

async function writeOnboardingProgress(businessId, fields) {
    try {
        await supabase
            .from('business_onboarding')
            .update({ ...fields, updated_at: new Date().toISOString() })
            .eq('business_id', businessId);
    } catch (e) {
        console.error(`  [Onboarding] Write failed: ${e.message}`);
    }
}

async function writeSyncStatus(businessId, stage, message, extra = {}) {
    const payload = { stage, message, updated_at: new Date().toISOString(), ...extra };
    console.log(`  [SyncStatus] ${stage}: ${message}`);
    await writeOnboardingProgress(businessId, { sync_status: payload });
}

/**
 * Read the current onboarding row.
 * Used as idempotency guard before auto-activation and product auto-approval.
 */
async function getOnboardingRow(businessId) {
    const { data } = await supabase
        .from('business_onboarding')
        .select(
            'leads_auto_activated_count, leads_auto_activated_at, ' +
            'leads_user_activated_count, products_auto_approved_count'
        )
        .eq('business_id', businessId)
        .single();
    return data || {};
}

// ============================================================
// SECTION 3 — HELPERS
// ============================================================

function isGroupOrBroadcast(jid) {
    if (!jid) return true;
    return (
        jid.endsWith('@g.us')      ||
        jid.endsWith('@broadcast') ||
        jid === 'status@broadcast' ||
        jid.includes('newsletter') ||
        jid.includes('lid')
    );
}

function extractPhone(jid) {
    if (!jid) return null;
    const raw = jid.split('@')[0].replace(/\D/g, '');
    if (!raw || raw.length < 7) return null;
    return `+${raw}`;
}

function extractMessageContent(msg) {
    const m = msg.message;
    if (!m) return null;

    if (m.conversation)
        return { text: m.conversation, type: 'text' };
    if (m.extendedTextMessage?.text)
        return { text: m.extendedTextMessage.text, type: 'text', _contextInfo: m.extendedTextMessage.contextInfo || null };
    if (m.imageMessage)
        return { text: m.imageMessage.caption || '', type: 'image', caption: m.imageMessage.caption || '' };
    if (m.audioMessage)
        return { text: '', type: m.audioMessage.ptt ? 'voice_note' : 'audio', duration_seconds: m.audioMessage.seconds || null };
    if (m.videoMessage)
        return { text: m.videoMessage.caption || '', type: 'video', caption: m.videoMessage.caption || '' };
    if (m.documentMessage)
        return { text: m.documentMessage.fileName || '', type: 'document', file_name: m.documentMessage.fileName || '' };
    if (m.stickerMessage)
        return { text: '', type: 'sticker' };
    if (m.reactionMessage)
        return { text: m.reactionMessage.text || '', type: 'reaction', emoji: m.reactionMessage.text || '' };
    if (m.locationMessage)
        return {
            text: `Location: ${m.locationMessage.degreesLatitude}, ${m.locationMessage.degreesLongitude}`,
            type: 'location',
            latitude: m.locationMessage.degreesLatitude,
            longitude: m.locationMessage.degreesLongitude
        };
    if (m.contactMessage)
        return { text: m.contactMessage.displayName || 'Contact shared', type: 'contact' };
    if (m.buttonsResponseMessage)
        return { text: m.buttonsResponseMessage.selectedDisplayText || '', type: 'button_response' };
    if (m.listResponseMessage)
        return { text: m.listResponseMessage.title || '', type: 'list_response' };

    return null;
}

// ============================================================
// SECTION 4 — PRE-SCAN
// ============================================================
// Runs BEFORE any DB work. O(n) pass over the raw messages array.
// Extracts:
//   - count of unique non-group inbound JIDs  → leads_total_found
//   - count of unique outbound image JIDs      → products raw count
// Count is written to onboarding immediately so the frontend
// can render the slider before the full sync completes.
// ============================================================

function preScanPayload(payload) {
    const messages = payload.data?.messages || [];
    const contacts = payload.data?.contacts || [];

    const cutoffTs               = Math.floor(Date.now() / 1000) - (HISTORY_DAYS * 24 * 60 * 60);
    const uniqueJids             = new Set();
    const outboundImgThumbHashes = new Set();
    let outboundImageCount       = 0;

    for (const msg of messages) {
        const jid = msg.key?.remoteJid;
        if (!jid || isGroupOrBroadcast(jid))          continue;
        if ((msg.messageTimestamp || 0) < cutoffTs)   continue;
        if (msg.messageStubType)                       continue;

        if (!msg.key.fromMe) uniqueJids.add(jid);

        if (msg.key.fromMe && msg.message?.imageMessage) {
            const thumb = msg.message.imageMessage.jpegThumbnail;
            if (thumb) {
                const key = typeof thumb === 'string'
                    ? thumb.substring(0, 50)
                    : JSON.stringify(thumb).substring(0, 50);
                if (!outboundImgThumbHashes.has(key)) {
                    outboundImgThumbHashes.add(key);
                    outboundImageCount++;
                }
            }
        }
    }

    const contactCount = contacts.filter(c => c.id && !isGroupOrBroadcast(c.id)).length;
    const leadsFound   = Math.max(uniqueJids.size, contactCount);

    return { leadsFound, uniqueJids, outboundImageCount, outboundImgThumbHashes };
}

// ============================================================
// SECTION 5 — WHATSAPP BUSINESS PROFILE
// ============================================================

async function fetchAndSaveWhatsAppProfile(businessId, instanceToken, jid) {
    try {
        console.log(`  [Profile] Fetching WA Business profile for ${businessId}`);

        const res = await fetch(`${EVOLUTION_URL}/chat/fetchProfile`, {
            method:  'POST',
            headers: { 'Content-Type': 'application/json', 'apikey': instanceToken },
            body:    JSON.stringify({ number: jid })
        });

        if (!res.ok) { console.warn(`  [Profile] fetchProfile returned ${res.status}`); return; }

        const profile = await res.json();
        const data    = profile?.data || profile;

        const { data: existing } = await supabase
            .from('businesses')
            .select('name, phone, owner_email, address, description, profile_picture_url, website_url')
            .eq('business_id', businessId)
            .single();

        const updates          = {};
        const namePlaceholders = ['', 'processing...', 'my business', null, undefined];
        if (namePlaceholders.includes((existing?.name || '').toLowerCase())) {
            const waName = data.name || data.pushName || data.verifiedName;
            if (waName) updates.name = waName;
        }
        if (data.picture || data.profilePictureUrl) updates.profile_picture_url = data.picture || data.profilePictureUrl;
        if (!existing?.address     && data.address)     updates.address     = data.address;
        if (!existing?.description && data.description) updates.description = data.description;
        if (!existing?.owner_email && data.email)       updates.owner_email = data.email;
        if (!existing?.website_url && data.website)     updates.website_url = data.website;
        if (!existing?.phone       && data.phone)       updates.phone       = data.phone;

        if (Object.keys(updates).length > 0) {
            await supabase.from('businesses').update(updates).eq('business_id', businessId);
            console.log(`  [Profile] ✓ Updated: ${Object.keys(updates).join(', ')}`);
        } else {
            console.log(`  [Profile] All fields populated — no updates`);
        }
    } catch (e) {
        console.error(`  [Profile] Error: ${e.message}`);
    }
}

// ============================================================
// SECTION 6 — AD ATTRIBUTION
// ============================================================

function extractAdAttribution(msg) {
    const m = msg.message;
    if (!m) return null;
    const contextInfo =
        m.extendedTextMessage?.contextInfo ||
        m.imageMessage?.contextInfo        ||
        m.videoMessage?.contextInfo        ||
        null;
    const adReply = contextInfo?.externalAdReply;
    if (!adReply) return null;

    let adPlatform = 'meta';
    const src      = (adReply.sourceUrl || '').toLowerCase();
    if (src.includes('instagram'))                            adPlatform = 'instagram';
    else if (src.includes('facebook') || src.includes('fb.com')) adPlatform = 'facebook';

    return {
        ad_id:            adReply.sourceId    || null,
        ad_headline:      adReply.title        || null,
        ad_body:          adReply.body         || null,
        ad_thumbnail_url: adReply.thumbnailUrl || null,
        ad_source_url:    adReply.sourceUrl    || null,
        ad_platform:      adPlatform,
        captured_at:      new Date().toISOString()
    };
}

async function recordAdAttribution(businessId, contactId, adData) {
    if (!adData?.ad_id) return null;
    try {
        const { data: adRecord, error } = await supabase
            .from('ad_attributions')
            .upsert({
                business_id:      businessId,
                ad_id:            adData.ad_id,
                ad_headline:      adData.ad_headline,
                ad_body:          adData.ad_body,
                ad_thumbnail_url: adData.ad_thumbnail_url,
                ad_source_url:    adData.ad_source_url,
                ad_platform:      adData.ad_platform,
            }, { onConflict: 'business_id, ad_id', ignoreDuplicates: false })
            .select('id')
            .single();

        if (error) { console.error(`  [Ad] Upsert failed: ${error.message}`); return null; }

        await supabase.rpc('increment_ad_lead_count', {
            p_ad_id: adRecord.id, p_business_id: businessId
        }).catch(e => console.warn(`  [Ad] Lead count RPC failed: ${e.message}`));

        await supabase.from('contacts').update({
            is_ad_lead:        true,
            lead_type:         'business',
            ad_attribution_id: adRecord.id,
            original_ad_id:    adData.ad_id,
            ad_headline:       adData.ad_headline,
            ad_platform:       adData.ad_platform,
            ad_attributed_at:  adData.captured_at
        }).eq('id', contactId);

        return adRecord.id;
    } catch (e) {
        console.error(`  [Ad] recordAdAttribution error: ${e.message}`);
        return null;
    }
}

// ============================================================
// SECTION 7 — LEAD CLASSIFICATION
// ============================================================
// The cleaner ONLY classifies leads if there is 100% structural certainty.
// Dynamic conversation text is left as 'pending_analysis' so that the 
// Enrichment Worker can accurately classify it without causing frontend deletion bugs.
// ============================================================

function classifyLeadType(firstMessageText, hasAdAttribution) {
    // Rule 1: If it brought ad attribution data, it is definitively a business interaction
    if (hasAdAttribution) return 'business';
    
    if (!firstMessageText) return 'pending_analysis';
    const text = firstMessageText.toLowerCase().trim();

    // Rule 2: Check for structural click-to-whatsapp ad text pre-fills (common in Meta ads)
    // E.g., "I saw this on Instagram...", "Please send more details about..."
    const adPrefillPatterns = [
        /i saw this on/i,
        /saw this product/i,
        /mnauza hii/i,
        /nimeona hii/i,
        /tuma picha/i
    ];
    for (const p of adPrefillPatterns) {
        if (p.test(text)) return 'business';
    }

    // Rule 3: No structural certainty? Leave it for the Enrichment Worker to figure out.
    // DO NOT label as 'personal' or 'unknown' here to protect frontend filters.
    return 'pending_analysis';
}

// ============================================================
// SECTION 8 — CONTACT + CONVERSATION HELPERS
// ============================================================

async function getOrCreateContact(businessId, jid, pushName) {
    const phone = extractPhone(jid);
    const { data: existing } = await supabase
        .from('contacts')
        .select('id, lead_state, name, is_ad_lead, lead_type, original_ad_id, ad_attribution_id')
        .eq('business_id', businessId)
        .eq('social_platform', 'whatsapp')
        .eq('social_id', jid)
        .maybeSingle();

    if (existing) {
        const updates = { last_seen: new Date().toISOString() };
        if (pushName && (!existing.name || existing.name === 'Unknown')) updates.name = pushName;
        await supabase.from('contacts').update(updates).eq('id', existing.id);
        return existing;
    }

    const { data: created, error } = await supabase
        .from('contacts')
        .insert({
            business_id:     businessId,
            social_platform: 'whatsapp',
            social_id:       jid,
            name:            pushName || 'Unknown',
            phone,
            lead_state:      'new',
            lead_type:       'unknown',
            is_ad_lead:      false,
            follow_up_count: 0,
            last_seen:       new Date().toISOString()
        })
        .select('id, lead_state, name, is_ad_lead, lead_type, original_ad_id, ad_attribution_id')
        .single();

    if (error) throw new Error(`Contact create failed: ${error.message}`);
    return created;
}

async function getOrCreateConversation(businessId, contactId, jid) {
    const { data: existing } = await supabase
        .from('conversations')
        .select('id')
        .eq('business_id', businessId)
        .eq('contact_id', contactId)
        .maybeSingle();

    if (existing) return existing.id;

    const { data: created, error } = await supabase
        .from('conversations')
        .insert({
            business_id:  businessId,
            contact_id:   contactId,
            external_id:  jid,
            channel:      'whatsapp',
            type:         'dm',
            status:       'open',
            unread_count: 0,
            ai_enabled:   true,
            active_agent: 'manager'
        })
        .select('id')
        .single();

    if (error) throw new Error(`Conversation create failed: ${error.message}`);
    return created.id;
}

async function updateLeadStateOnReply(contactId, currentState) {
    const transitionStates = ['new', 'stalled', 'ghosted', 'warm'];
    if (!transitionStates.includes(currentState)) return;
    await supabase.from('contacts').update({ lead_state: 'engaged' }).eq('id', contactId);
}

async function cancelPendingFollowUps(contactId) {
    await supabase.from('follow_up_queue')
        .update({ status: 'cancelled', skip_reason: 'lead_replied' })
        .eq('contact_id', contactId)
        .eq('status', 'pending');
}

async function processConsentResponse(contactId, contentText) {
    const text   = contentText?.trim().toLowerCase() || '';
    const isStop = /\bstop\b|hapana|usiteme|acha|unsubscribe|opt.?out/i.test(text);
    const isYes  = /\byes\b|\bndio\b|\bok\b|\bsawa\b|subscribe|nipe|tuma/i.test(text);

    if (isStop) {
        await supabase.from('contacts').update({
            do_not_contact:         true,
            follow_up_opted_in:     false,
            follow_up_opted_out_at: new Date().toISOString()
        }).eq('id', contactId);
        await supabase.from('follow_up_queue')
            .update({ status: 'cancelled', skip_reason: 'contact_opted_out' })
            .eq('contact_id', contactId)
            .eq('status', 'pending');
    }

    if (isYes) {
        const { data: contact } = await supabase
            .from('contacts')
            .select('consent_message_sent_at, follow_up_opted_in')
            .eq('id', contactId)
            .single();
        if (contact?.consent_message_sent_at && !contact?.follow_up_opted_in) {
            await supabase.from('contacts').update({
                follow_up_opted_in:    true,
                follow_up_opted_in_at: new Date().toISOString()
            }).eq('id', contactId);
        }
    }
}

// ============================================================
// SECTION 9 — AUTO-ACTIVATION (first 30 leads)
// ============================================================
// Idempotency gate: reads leads_auto_activated_at from onboarding.
// Charges KES, writes onboarding stage, then fires InstructionsGenerator.
// Does NOT touch lead enrichment — that's LeadsEnrichment's job.
// ============================================================

async function autoActivateLeads(businessId, onboardingRow) {
    if (onboardingRow.leads_auto_activated_at) {
        console.log(`  [AutoActivate] Already activated for ${businessId} — skipping`);
        return { skipped: true };
    }

    const pricePerLead = await getBillingValue('lead_ingestion_min_kes', 0.75);
    const totalKes     = AUTO_ACTIVATE_LEADS * pricePerLead;

    console.log(`  [AutoActivate] Attempting ${AUTO_ACTIVATE_LEADS} leads @ ${pricePerLead} KES = ${totalKes} KES`);

    await writeSyncStatus(businessId, 'auto_activating',
        `Activating your first ${AUTO_ACTIVATE_LEADS} leads...`,
        { auto_count: AUTO_ACTIVATE_LEADS }
    );

    const charge = await chargeKes(
        businessId,
        totalKes,
        `Auto-activation — first ${AUTO_ACTIVATE_LEADS} leads`
    );

    if (!charge.ok) {
        console.warn(`  [AutoActivate] Charge failed: ${charge.reason}`);
        await writeSyncStatus(businessId, 'insufficient_funds_auto',
            `Top up your balance to activate leads. First ${AUTO_ACTIVATE_LEADS} leads cost ${totalKes} KES.`,
            { required_kes: totalKes, reason: charge.reason }
        );
        return { ok: false, reason: charge.reason };
    }

    // Write idempotency lock first
    await writeOnboardingProgress(businessId, {
        leads_auto_activated_count: AUTO_ACTIVATE_LEADS,
        leads_auto_activated_at:    new Date().toISOString(),
        leads_processed:            AUTO_ACTIVATE_LEADS,
        leads_user_activated_count: 0,
        current_step:               4,
        sync_status: {
            stage:       'leads_activated',
            message:     `${AUTO_ACTIVATE_LEADS} leads activated automatically. Your AI bot is being built...`,
            auto_count:  AUTO_ACTIVATE_LEADS,
            charged_kes: totalKes,
            updated_at:  new Date().toISOString()
        }
    });

    console.log(`  [AutoActivate] ✓ ${AUTO_ACTIVATE_LEADS} leads activated, charged ${totalKes} KES`);

    // Fire InstructionsGenerator (bot build) — non-blocking
    triggerInstructionsGenerator(businessId).catch(e =>
        console.error('[AutoActivate] InstructionsGenerator trigger error:', e.message)
    );

    // Fire LeadsEnrichment — non-blocking
    // Enrichment runs independently; it doesn't block the onboarding flow
    triggerLeadsEnrichment(businessId).catch(e =>
        console.error('[AutoActivate] LeadsEnrichment trigger error:', e.message)
    );

    return { ok: true, activated: AUTO_ACTIVATE_LEADS, charged_kes: totalKes, charged_usd: charge.charged_usd };
}

// ============================================================
// SECTION 10 — HISTORY SYNC
// ============================================================
// Phase 1 (pre-scan)   — O(n), no DB — writes leads_total_found immediately
// Phase 2 (DB upserts) — contacts, conversations, messages in bulk
// Phase 3 (classify)   — ad attribution + lead type per contact
// Phase 4 (activate)   — charge + fire InstructionsGenerator + LeadsEnrichment
// Phase 5 (products)   — stage images + fire ProductsHandler
// ============================================================

async function processHistorySync(payload, businessId, sasaBusinessId) {
    const contacts = payload.data?.contacts || [];
    const chats    = payload.data?.chats    || [];
    const messages = payload.data?.messages || [];

    const stats = { contacts: 0, conversations: 0, messages: 0, skipped: 0, ad_leads: 0 };
    console.log(`  [History] Raw — contacts:${contacts.length} chats:${chats.length} messages:${messages.length}`);

    const onboardingRow = await getOnboardingRow(businessId);

    // ── Phase 1: Pre-scan — write count before any DB work ─────
    await writeSyncStatus(businessId, 'history_received',
        `Received your WhatsApp history. Counting contacts...`
    );

    const preScan = preScanPayload(payload);

    await writeOnboardingProgress(businessId, {
        leads_total_found: preScan.leadsFound,
        sync_status: {
            stage:        'awaiting_lead_activation',
            message:      `Found ${preScan.leadsFound} contacts from the last ${HISTORY_DAYS} days. Processing messages...`,
            total_found:  preScan.leadsFound,
            products_raw: preScan.outboundImageCount,
            updated_at:   new Date().toISOString()
        }
    });

    console.log(`  [History] Pre-scan: ${preScan.leadsFound} leads, ${preScan.outboundImageCount} product images`);

    // ── Phase 2: Contacts upsert ────────────────────────────────
    const validContacts   = contacts.filter(c => c.id && !isGroupOrBroadcast(c.id));
    const contactPayloads = validContacts.map(c => ({
        business_id:     businessId,
        social_platform: 'whatsapp',
        social_id:       c.id,
        name:            c.name || c.notify || c.verifiedName || 'Unknown',
        phone:           extractPhone(c.id),
        lead_state:      'new',
        lead_type:       'pending_analysis',
        is_ad_lead:      false
    }));

    let contactMap = {};
    if (contactPayloads.length > 0) {
        const { data: inserted, error } = await supabase
            .from('contacts')
            .upsert(contactPayloads, {
                onConflict:       'business_id, social_platform, social_id',
                ignoreDuplicates: false
            })
            .select('id, social_id');
        if (error) throw new Error(`Contacts upsert: ${error.message}`);
        contactMap     = inserted.reduce((acc, c) => { acc[c.social_id] = c.id; return acc; }, {});
        stats.contacts = inserted.length;
        console.log(`  [History] ${stats.contacts} contacts upserted`);
    }

    // ── Phase 2b: Conversations upsert ─────────────────────────
    const validChats           = chats.filter(c => c.id && !isGroupOrBroadcast(c.id));
    const conversationPayloads = validChats
        .map(chat => ({
            business_id:  businessId,
            contact_id:   contactMap[chat.id],
            external_id:  chat.id,
            channel:      'whatsapp',
            type:         'dm',
            status:       (chat.unreadCount || 0) > 0 ? 'open' : 'closed',
            unread_count: chat.unreadCount || 0,
            ai_enabled:   true,
            active_agent: 'manager'
        }))
        .filter(c => c.contact_id != null);

    let convoMap = {};
    if (conversationPayloads.length > 0) {
        const { data: inserted, error } = await supabase
            .from('conversations')
            .upsert(conversationPayloads, { onConflict: 'business_id, contact_id' })
            .select('id, external_id');
        if (error) throw new Error(`Conversations upsert: ${error.message}`);
        convoMap            = inserted.reduce((acc, c) => { acc[c.external_id] = c.id; return acc; }, {});
        stats.conversations = inserted.length;
    }

    // ── Phase 2c: Messages upsert ───────────────────────────────
    const cutoffTs        = Math.floor(Date.now() / 1000) - (HISTORY_DAYS * 24 * 60 * 60);
    const convLastMsg     = {};
    const convLastInbound = {};
    const messagePayloads = [];
    const contactFirstMsg = {};
    const outboundImages  = [];
    const seenThumbHashes = preScan.outboundImgThumbHashes;

    for (const msg of messages) {
        const jid = msg.key?.remoteJid;
        if (!jid || isGroupOrBroadcast(jid))        { stats.skipped++; continue; }
        if ((msg.messageTimestamp || 0) < cutoffTs) { stats.skipped++; continue; }
        if (msg.messageStubType)                     { stats.skipped++; continue; }

        const conversationId = convoMap[jid];
        const contactId      = contactMap[jid] || null;
        if (!conversationId)                         { stats.skipped++; continue; }

        const content  = extractMessageContent(msg);
        if (!content)                                { stats.skipped++; continue; }

        const isFromMe = msg.key.fromMe === true;
        const ts       = msg.messageTimestamp;

        if (!convLastMsg[conversationId] || ts > convLastMsg[conversationId].ts) {
            convLastMsg[conversationId] = { ts, preview: (content.text || content.type || '').slice(0, 120) };
        }
        if (!isFromMe) {
            if (!convLastInbound[conversationId] || ts > convLastInbound[conversationId]) {
                convLastInbound[conversationId] = ts;
            }
            if (!contactFirstMsg[contactId] || ts < contactFirstMsg[contactId].ts) {
                contactFirstMsg[contactId] = { ts, msg, content };
            }
        }

        if (isFromMe && content.type === 'image') {
            const thumb = msg.message?.imageMessage?.jpegThumbnail;
            if (thumb) {
                const key = typeof thumb === 'string'
                    ? thumb.substring(0, 50)
                    : JSON.stringify(thumb).substring(0, 50);
                if (seenThumbHashes.has(key)) outboundImages.push(msg);
            }
        }

        messagePayloads.push({
            whatsapp_message_id: msg.key.id,
            business_id:         businessId,
            contact_id:          contactId,
            conversation_id:     conversationId,
            direction:           isFromMe ? 'out' : 'in',
            role:                isFromMe ? 'admin' : 'user',
            agent_role:          isFromMe ? 'human' : 'legacy_ai',
            type:                content.type,
            content,
            created_at:          new Date(ts * 1000).toISOString(),
            status:              'sent',
            is_read:             isFromMe,
            raw_payload:         msg
        });
    }

    for (let i = 0; i < messagePayloads.length; i += 100) {
        const chunk = messagePayloads.slice(i, i + 100);
        const { error } = await supabase
            .from('messages')
            .upsert(chunk, { onConflict: 'whatsapp_message_id' });
        if (!error) stats.messages += chunk.length;
    }
    console.log(`  [History] ${stats.messages} messages upserted, ${stats.skipped} skipped`);

    // ── Phase 3: Classify leads + ad attribution ────────────────
    await writeSyncStatus(businessId, 'classifying_leads',
        `Classifying ${Object.keys(contactFirstMsg).length} leads...`
    );

    for (const [contactId, { msg, content }] of Object.entries(contactFirstMsg)) {
        try {
            const adData      = extractAdAttribution(msg);
            const leadType    = classifyLeadType(content.text, !!adData);
            const interests   = extractProductInterests(adData, content.text);
            const contactUpdate = { lead_type: leadType };
            if (interests.length > 0) contactUpdate.product_interests = interests;
            await supabase.from('contacts').update(contactUpdate).eq('id', contactId);
            if (adData) { await recordAdAttribution(businessId, contactId, adData); stats.ad_leads++; }
        } catch (e) {
            console.error(`  [Classify] Error for contact ${contactId}: ${e.message}`);
        }
    }

    // ── Phase 3b: Update conversation metadata ──────────────────
    for (const [convId, data] of Object.entries(convLastMsg)) {
        const fields = { last_message_preview: data.preview };
        if (convLastInbound[convId]) {
            fields.last_user_message_at = new Date(convLastInbound[convId] * 1000).toISOString();
        }
        await supabase.from('conversations').update(fields).eq('id', convId);
    }

    // ── Phase 4: Auto-activate + fire workers ───────────────────
    // autoActivateLeads internally fires InstructionsGenerator + LeadsEnrichment
    await autoActivateLeads(businessId, onboardingRow);

    // ── Phase 5: Stage product images + fire ProductsHandler ────
    if (outboundImages.length > 0) {
        await stageAndAutoApproveProductImages(businessId, outboundImages, onboardingRow);
    }

    console.log(`  [History] ✓ Full sync complete for ${businessId}`);
    return stats;
}

// ============================================================
// SECTION 11 — PRODUCT IMAGE STAGING
// ============================================================
// Responsibility: write product_image_staging rows and update onboarding counts.
// Does NOT download images or call OpenAI — that's ProductsHandler's job.
//
// After staging, fires ProductsHandler non-blocking with the list of
// approved media_urls. ProductsHandler does the download → vision → products insert.
//
// Idempotency gate: products_auto_approved_count > 0 means first run already happened.
// ============================================================

async function stageAndAutoApproveProductImages(businessId, imageMessages, onboardingRow) {
    try {
        const alreadyAutoApproved = onboardingRow.products_auto_approved_count || 0;
        const isFirstRun          = alreadyAutoApproved === 0;

        console.log(`  [Products] Staging ${imageMessages.length} unique images (first run: ${isFirstRun})`);

        const { data: existingStaged } = await supabase
            .from('product_image_staging')
            .select('media_url, status')
            .eq('business_id', businessId);

        const existingUrls = new Set((existingStaged || []).map(r => r.media_url).filter(Boolean));

        const newImages = imageMessages.filter(msg => {
            const url = msg.message?.imageMessage?.url;
            return url && !existingUrls.has(url);
        });

        console.log(`  [Products] ${newImages.length} truly new images (${imageMessages.length - newImages.length} already staged)`);

        if (newImages.length === 0) {
            console.log(`  [Products] Nothing new to stage — skipping`);
            return;
        }

        let autoApproveSlots = isFirstRun ? AUTO_APPROVE_PRODUCTS - alreadyAutoApproved : 0;
        autoApproveSlots     = Math.max(0, Math.min(autoApproveSlots, AUTO_APPROVE_PRODUCTS));

        const toAutoApprove = newImages.slice(0, autoApproveSlots);
        const toPending     = newImages.slice(autoApproveSlots);
        const now           = new Date().toISOString();

        const buildPayload = (msg, status) => ({
            business_id:      businessId,
            media_url:        msg.message?.imageMessage?.url             || null,
            thumbnail:        msg.message?.imageMessage?.jpegThumbnail   || null,
            caption:          msg.message?.imageMessage?.caption         || null,
            raw_payload:      msg,
            status,
            is_auto_approved: status === 'approved',
            approved_at:      status === 'approved' ? now : null,
            created_at:       now
        });

        const stagingPayloads = [
            ...toAutoApprove.map(msg => buildPayload(msg, 'approved')),
            ...toPending.map(msg => buildPayload(msg, 'pending_approval'))
        ];

        if (stagingPayloads.length > 0) {
            const { error } = await supabase
                .from('product_image_staging')
                .upsert(stagingPayloads, {
                    onConflict:       'business_id, media_url',
                    ignoreDuplicates: true
                });
            if (error) console.warn(`  [Products] Staging upsert warning: ${error.message}`);
        }

        const newAutoApproved    = alreadyAutoApproved + toAutoApprove.length;
        const totalPendingResult = await supabase
            .from('product_image_staging')
            .select('id', { count: 'exact', head: true })
            .eq('business_id', businessId)
            .eq('status', 'pending_approval');
        const pendingCount = totalPendingResult.count || toPending.length;

        const progressFields = {
            products_auto_approved_count: newAutoApproved,
            products_pending_approval:    pendingCount
        };

        if (isFirstRun && toAutoApprove.length > 0) {
            progressFields.products_seeded  = true;
            progressFields.products_done_at = now;
            await writeOnboardingProgress(businessId, {
                ...progressFields,
                sync_status: {
                    stage:         'products_auto_approved',
                    message:       `${toAutoApprove.length} product images sent for analysis automatically.` +
                                   (pendingCount > 0 ? ` ${pendingCount} more waiting for your approval.` : ''),
                    auto_approved: toAutoApprove.length,
                    pending:       pendingCount,
                    updated_at:    now
                }
            });

            // Fire ProductsHandler for the approved batch — non-blocking
            // Pass the approved media_urls so it doesn't need to re-query
            const approvedUrls = toAutoApprove
                .map(msg => msg.message?.imageMessage?.url)
                .filter(Boolean);

            triggerProductsHandler(businessId, approvedUrls).catch(e =>
                console.error('[Products] ProductsHandler trigger error:', e.message)
            );

        } else if (pendingCount > 0) {
            await writeOnboardingProgress(businessId, {
                ...progressFields,
                sync_status: {
                    stage:         'products_discovered',
                    message:       `${pendingCount} product images found. Approve them to add to your catalog.`,
                    auto_approved: newAutoApproved,
                    pending:       pendingCount,
                    updated_at:    now
                }
            });
        } else {
            await writeOnboardingProgress(businessId, progressFields);
        }

        console.log(`  [Products] ✓ Auto-approved: ${toAutoApprove.length}, Pending: ${pendingCount}`);

    } catch (e) {
        console.error(`  [Products] stageAndAutoApproveProductImages error: ${e.message}`);
    }
}

// ============================================================
// SECTION 12 — LIVE MESSAGE HANDLER
// ============================================================

async function processLiveMessage(payload, businessId) {
    let rawMessages = [];
    if (Array.isArray(payload.data))  rawMessages = payload.data;
    else if (payload.data?.key)       rawMessages = [payload.data];
    else if (payload.data?.messages)  rawMessages = payload.data.messages;
    else { console.log('  [Live] No parseable messages'); return; }

    for (const msg of rawMessages) {
        try {
            const jid = msg.key?.remoteJid;
            if (!jid || isGroupOrBroadcast(jid)) continue;
            if (msg.messageStubType) continue;
            if (!msg.key?.id) continue;

            const isFromMe   = msg.key.fromMe === true;
            const pushName   = msg.pushName || null;
            const ts         = msg.messageTimestamp || Math.floor(Date.now() / 1000);
            const timestamp  = new Date(ts * 1000).toISOString();

            const contact        = await getOrCreateContact(businessId, jid, pushName);
            const contactId      = contact.id;
            const conversationId = await getOrCreateConversation(businessId, contactId, jid);

            const content = extractMessageContent(msg);
            if (!content) continue;

            let adAttributionId = null;
            if (!isFromMe) {
                const adData = extractAdAttribution(msg);
                if (adData && !contact.is_ad_lead) {
                    adAttributionId = await recordAdAttribution(businessId, contactId, adData);
                } else if (contact.ad_attribution_id) {
                    adAttributionId = contact.ad_attribution_id;
                }
                if (!contact.lead_type || contact.lead_type === 'unknown') {
                    const leadType  = classifyLeadType(content.text, !!adData);
                    const interests = extractProductInterests(adData, content.text);
                    const update    = { lead_type: leadType };
                    if (interests.length > 0) update.product_interests = interests;
                    await supabase.from('contacts').update(update).eq('id', contactId);
                }
            }

            await supabase.from('messages').upsert({
                whatsapp_message_id: msg.key.id,
                business_id:         businessId,
                contact_id:          contactId,
                conversation_id:     conversationId,
                direction:           isFromMe ? 'out' : 'in',
                role:                isFromMe ? 'admin' : 'user',
                agent_role:          isFromMe ? 'human' : 'legacy_ai',
                type:                content.type,
                content,
                created_at:          timestamp,
                status:              'sent',
                is_read:             isFromMe,
                raw_payload:         msg
            }, { onConflict: 'whatsapp_message_id' });

            const preview    = (content.text || content.type || '').slice(0, 120);
            const convUpdate = { last_message_preview: preview, status: 'open' };
            if (!isFromMe) convUpdate.last_user_message_at = timestamp;
            await supabase.from('conversations').update(convUpdate).eq('id', conversationId);

            if (!isFromMe) {
                await updateLeadStateOnReply(contactId, contact.lead_state);
                await cancelPendingFollowUps(contactId);
                await processConsentResponse(contactId, content.text);
            }

            console.log(
                `  [Live] ✓ ${isFromMe ? 'OUT' : 'IN '} | ` +
                `type:${content.type.padEnd(12)} | contact:${contactId}` +
                (adAttributionId ? ` | ad:${adAttributionId}` : '')
            );

        } catch (msgErr) {
            console.error(`  [Live] Error processing message: ${msgErr.message}`);
        }
    }
}

// ============================================================
// SECTION 13 — MESSAGE STATUS UPDATE HANDLER
// ============================================================

async function processMessageStatusUpdate(payload, businessId) {
    try {
        const response = await fetch(`${ENRICHMENT_WORKER_URL}/status-update`, {
            method:  'POST',
            headers: { 'Content-Type': 'application/json' },
            body:    JSON.stringify({ business_id: businessId, ...payload })
        });
        if (!response.ok) console.warn(`  [StatusUpdate] Enrichment worker returned ${response.status}`);
        else              console.log(`  [StatusUpdate] ✓ Forwarded`);
    } catch (e) {
        console.error(`  [StatusUpdate] Failed to forward: ${e.message}`);
    }
}

// ============================================================
// SECTION 14 — CONNECTION UPDATE HANDLER
// ============================================================

async function processConnectionUpdate(payload, businessId) {
    const state        = payload.data?.state;
    const statusReason = payload.data?.statusReason;
    const instanceToken = new URL(payload.webhookUrl || 'http://x').searchParams.get('sasa_business_id')
        || payload.sasa_business_id
        || null;

    console.log(`  [Connection] ${businessId} → state:${state} reason:${statusReason || 'n/a'}`);

    if (state === 'open') {
        try {
            const { data: biz } = await supabase
                .from('businesses')
                .select('ai_config')
                .eq('business_id', businessId)
                .single();

            await supabase.from('businesses').update({
                ai_config: {
                    ...(biz?.ai_config || {}),
                    whatsapp_status:       'connected',
                    whatsapp_connected_at: new Date().toISOString()
                }
            }).eq('business_id', businessId);
        } catch (e) {
            console.error(`  [Connection] ai_config update failed: ${e.message}`);
        }

        await writeOnboardingProgress(businessId, {
            whatsapp_connected: true,
            wa_connected_at:    new Date().toISOString(),
            current_step:       3,
            sync_status: {
                stage:      'connected',
                message:    'WhatsApp connected. Fetching your business profile and message history...',
                updated_at: new Date().toISOString()
            }
        });

        if (instanceToken) {
            try {
                const { data: biz } = await supabase
                    .from('businesses')
                    .select('phone')
                    .eq('business_id', businessId)
                    .single();
                const jid = biz?.phone?.replace(/\D/g, '') + '@s.whatsapp.net';
                fetchAndSaveWhatsAppProfile(businessId, instanceToken, jid).catch(() => {});
            } catch (e) {
                console.warn(`  [Profile] Could not determine JID: ${e.message}`);
            }
        }
    }

    if (state === 'close') {
        try {
            const { data: biz } = await supabase
                .from('businesses')
                .select('ai_config')
                .eq('business_id', businessId)
                .single();

            await supabase.from('businesses').update({
                ai_config: {
                    ...(biz?.ai_config || {}),
                    whatsapp_status:          'disconnected',
                    whatsapp_disconnected_at: new Date().toISOString()
                }
            }).eq('business_id', businessId);
        } catch (e) {
            console.error(`  [Connection] Disconnect update failed: ${e.message}`);
        }

        await writeSyncStatus(businessId, 'disconnected',
            'WhatsApp disconnected. Please reconnect from settings.'
        );
    }
}

// ============================================================
// SECTION 15 — WORKER TRIGGERS
// ============================================================
// All three workers are fired non-blocking (fire-and-forget HTTP POST).
// The cleaner does not await their completion — it just fires and moves on.
// Each worker is responsible for its own error handling and status writes.
//
// InstructionsGenerator: builds persona pack (voice + context + customer analysis)
// ProductsHandler:       downloads images, runs vision, inserts into products table
// LeadsEnrichment:       enriches contacts with intent, quality score, lead type
// ============================================================

async function triggerInstructionsGenerator(businessId) {
    try {
        await writeSyncStatus(businessId, 'bot_building',
            'Building your AI bot... This takes 1–2 minutes. You can continue setup in the meantime.'
        );

        await supabase.from('businesses')
            .update({ persona_pack_status: 'running' })
            .eq('business_id', businessId);

        const res = await fetch(`${INSTRUCTIONS_URL}/generate-persona`, {
            method:  'POST',
            headers: { 'Content-Type': 'application/json' },
            body:    JSON.stringify({ business_id: businessId })
        });

        if (res.ok) {
            console.log(`  [Workers] ✓ InstructionsGenerator triggered for ${businessId}`);
        } else if (res.status === 402) {
            const body = await res.json();
            console.warn(`  [Workers] InstructionsGenerator insufficient funds: ${body.message}`);
            await writeSyncStatus(businessId, 'bot_build_failed',
                'Insufficient credits to build AI bot. Please top up.'
            );
            await supabase.from('businesses')
                .update({ persona_pack_status: 'insufficient_funds' })
                .eq('business_id', businessId);
        } else {
            console.error(`  [Workers] InstructionsGenerator returned ${res.status}`);
            await writeSyncStatus(businessId, 'bot_build_failed',
                'Bot build failed. Will retry automatically.'
            );
            await supabase.from('businesses')
                .update({ persona_pack_status: 'failed' })
                .eq('business_id', businessId);
        }
    } catch (e) {
        console.error(`  [Workers] InstructionsGenerator trigger error: ${e.message}`);
        await writeSyncStatus(businessId, 'bot_build_failed',
            'Bot build failed — network error. Will retry.'
        );
    }
}

async function triggerProductsHandler(businessId, approvedMediaUrls = []) {
    try {
        const res = await fetch(`${PRODUCTS_HANDLER_URL}/process-images`, {
            method:  'POST',
            headers: { 'Content-Type': 'application/json' },
            body:    JSON.stringify({
                business_id:        businessId,
                approved_media_urls: approvedMediaUrls   // pre-approved batch, skip re-querying
            })
        });

        if (res.ok) {
            console.log(`  [Workers] ✓ ProductsHandler triggered for ${businessId} (${approvedMediaUrls.length} images)`);
        } else {
            console.error(`  [Workers] ProductsHandler returned ${res.status}`);
        }
    } catch (e) {
        console.error(`  [Workers] ProductsHandler trigger error: ${e.message}`);
    }
}

async function triggerLeadsEnrichment(businessId) {
    try {
        const res = await fetch(`${LEADS_ENRICHMENT_URL}/enrich`, {
            method:  'POST',
            headers: { 'Content-Type': 'application/json' },
            body:    JSON.stringify({ business_id: businessId })
        });

        if (res.ok) {
            console.log(`  [Workers] ✓ LeadsEnrichment triggered for ${businessId}`);
        } else {
            console.error(`  [Workers] LeadsEnrichment returned ${res.status}`);
        }
    } catch (e) {
        console.error(`  [Workers] LeadsEnrichment trigger error: ${e.message}`);
    }
}

// ============================================================
// SECTION 16 — EXPRESS ROUTES
// ============================================================

// ── Webhook from Evolution API ────────────────────────────────
app.post('/webhook/evolution', async (req, res) => {
    const businessId     = req.query.business_id;
    const sasaBusinessId = req.query.sasa_business_id;
    const eventType      = req.body?.event;

    if (!businessId) return res.status(400).json({ error: 'Missing business_id query param' });

    res.status(200).send('OK');  // Always ack immediately — Evolution retries on non-200

    console.log(`\n[Webhook] event:${eventType} | business:${businessId}`);

    try {
        switch (eventType) {
            case 'messaging-history.set':
                await processHistorySync(req.body, businessId, sasaBusinessId);
                console.log(`[History] ✓ Full sync complete for ${businessId}`);
                break;
            case 'messages.upsert':
                await processLiveMessage(req.body, businessId);
                break;
            case 'messages.update':
                await processMessageStatusUpdate(req.body, businessId);
                break;
            case 'connection.update':
                req.body.sasa_business_id = sasaBusinessId;
                await processConnectionUpdate(req.body, businessId);
                break;
            default:
                console.log(`[Webhook] Unhandled event: ${eventType}`);
        }
    } catch (err) {
        console.error(`[Webhook] Error handling ${eventType} for ${businessId}: ${err.message}`);
    }
});

// ── Lead activation (slider → activate MORE leads beyond auto-30) ─
//
// Body: { business_id, count }
//   count = TOTAL leads desired (e.g. 1500)
//   delta = count - (auto_activated + user_activated_so_far)
//   → charge only the delta
//   → update leads_user_activated_count and leads_processed
//
// Returns 400 if delta <= 0
// Returns 402 if insufficient balance
// ─────────────────────────────────────────────────────────────────
app.post('/leads/activate', async (req, res) => {
    const { business_id, count } = req.body;
    if (!business_id)        return res.status(400).json({ error: 'business_id required' });
    if (!count || count < 1) return res.status(400).json({ error: 'count must be >= 1' });

    try {
        const onboarding    = await getOnboardingRow(business_id);
        const autoActivated = onboarding.leads_auto_activated_count || 0;
        const userActivated = onboarding.leads_user_activated_count || 0;
        const alreadyTotal  = autoActivated + userActivated;
        const delta         = count - alreadyTotal;

        if (delta <= 0) {
            return res.status(400).json({
                error:             'already_activated',
                message:           `You have already activated ${alreadyTotal} leads. Request a higher number to add more.`,
                already_activated: alreadyTotal
            });
        }

        const pricePerLead = await getBillingValue('lead_ingestion_min_kes', 0.75);
        const totalKes     = delta * pricePerLead;

        const charge = await chargeKes(
            business_id,
            totalKes,
            `Lead top-up — ${delta} additional leads @ ${pricePerLead} KES each`
        );

        if (!charge.ok) {
            return res.status(402).json({
                error:        'insufficient_funds',
                required_kes: totalKes,
                delta,
                reason:       charge.reason
            });
        }

        const newUserActivated = userActivated + delta;
        const newTotal         = autoActivated + newUserActivated;

        await writeOnboardingProgress(business_id, {
            leads_user_activated_count: newUserActivated,
            leads_processed:            newTotal,
            sync_status: {
                stage:           'leads_activated',
                message:         `${newTotal} leads now activated (${delta} added just now).`,
                total_activated: newTotal,
                auto_activated:  autoActivated,
                user_activated:  newUserActivated,
                last_charged_kes: totalKes,
                updated_at:      new Date().toISOString()
            }
        });

        // Re-fire LeadsEnrichment for the expanded set — non-blocking
        triggerLeadsEnrichment(business_id).catch(e =>
            console.error('[LeadActivate] LeadsEnrichment re-trigger error:', e.message)
        );

        console.log(`  [LeadActivate] ✓ +${delta} leads for ${business_id} (total: ${newTotal}), charged ${totalKes} KES`);

        res.json({
            ok:              true,
            delta_activated: delta,
            total_activated: newTotal,
            charged_kes:     totalKes,
            charged_usd:     charge.charged_usd
        });

    } catch (e) {
        console.error('[LeadActivate] Error:', e.message);
        res.status(500).json({ error: e.message });
    }
});

// ── Product image approval (user manually approves pending images) ─
//
// Body: { business_id }
// Approves all pending_approval images.
// Sets products_seeded = true so ProductsHandler picks them up.
// ──────────────────────────────────────────────────────────────────
app.post('/products/approve-discovery', async (req, res) => {
    const { business_id } = req.body;
    if (!business_id) return res.status(400).json({ error: 'business_id required' });

    try {
        const now = new Date().toISOString();

        const { data: approved, error } = await supabase
            .from('product_image_staging')
            .update({ status: 'approved', approved_at: now })
            .eq('business_id', business_id)
            .eq('status', 'pending_approval')
            .select('id, media_url');

        if (error) throw error;

        const approvedCount = approved?.length || 0;
        console.log(`  [ProductApproval] ${approvedCount} images approved for ${business_id}`);

        const onboarding = await getOnboardingRow(business_id);
        const newTotal   = (onboarding.products_auto_approved_count || 0) + approvedCount;

        await writeOnboardingProgress(business_id, {
            products_auto_approved_count: newTotal,
            products_pending_approval:    0,
            products_seeded:              true,
            products_done_at:             now,
            sync_status: {
                stage:          'products_approved',
                message:        `${approvedCount} product images approved. They will be analysed and added to your catalog shortly.`,
                total_approved: newTotal,
                just_approved:  approvedCount,
                updated_at:     now
            }
        });

        // Fire ProductsHandler with the newly approved URLs
        const approvedUrls = (approved || []).map(r => r.media_url).filter(Boolean);
        if (approvedUrls.length > 0) {
            triggerProductsHandler(business_id, approvedUrls).catch(e =>
                console.error('[ProductApproval] ProductsHandler trigger error:', e.message)
            );
        }

        res.json({ ok: true, approved_count: approvedCount, total_approved: newTotal });

    } catch (e) {
        console.error('[ProductApproval] Error:', e.message);
        res.status(500).json({ error: e.message });
    }
});

// ── Health check ──────────────────────────────────────────────
app.get('/health', (_req, res) => res.json({
    status:   'ok',
    service:  'evolution-cleaner',
    version:  '4.0.0',
    workers: {
        instructions: INSTRUCTIONS_URL,
        products:     PRODUCTS_HANDLER_URL,
        enrichment:   LEADS_ENRICHMENT_URL
    }
}));

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`EvolutionCleaner v4 online — port ${PORT}`));
