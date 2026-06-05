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

const EVOLUTION_URL       = process.env.EVOLUTION_URL       || 'http://129.213.33.173:8080';
const EVOLUTION_API_KEY   = process.env.EVOLUTION_API_KEY   || 'mysupersecretapikey123';
const PERSONA_SERVICE_URL = process.env.PERSONA_SERVICE_URL || 'http://129.213.33.173:8002';
const HISTORY_DAYS        = parseInt(process.env.HISTORY_DAYS || '90');

// ==========================================
// 1. BILLING CONFIG
// ==========================================
// All prices drawn from followup_billing_config table at runtime.
// Keys we read:
//   lead_ingestion_min_kes       — cost per contact activated by slider (default 0.75)
//   usd_to_kes_rate              — exchange rate for KES→USD conversion (default 130)
//   bot_build_cost_multiplier    — multiplier on raw OpenAI cost for bot instructions (default 3)
//   bot_build_min_balance_usd    — minimum balance required to start bot build (default 0.10)

let _billingConfig = null;
let _billingConfigFetchedAt = 0;

async function getBillingConfig() {
    // Cache for 5 minutes
    if (_billingConfig && (Date.now() - _billingConfigFetchedAt) < 5 * 60 * 1000) {
        return _billingConfig;
    }
    try {
        const { data } = await supabase
            .from('followup_billing_config')
            .select('key, value');
        if (data) {
            _billingConfig = data.reduce((acc, row) => {
                acc[row.key] = row.value;
                return acc;
            }, {});
            _billingConfigFetchedAt = Date.now();
        }
    } catch (e) {
        console.error('[Billing] Config fetch failed:', e.message);
        _billingConfig = _billingConfig || {};
    }
    return _billingConfig || {};
}

async function getBillingValue(key, defaultValue) {
    const config = await getBillingConfig();
    const val = config[key];
    if (val === undefined || val === null) return defaultValue;
    const num = parseFloat(val);
    return isNaN(num) ? defaultValue : num;
}

// Deduct KES from business balance (converts KES→USD using config rate)
async function chargeKes(businessId, amountKes, description) {
    try {
        const rate = await getBillingValue('usd_to_kes_rate', 130);
        const amountUsd = amountKes / rate;

        const { data: balance } = await supabase
            .from('business_balances')
            .select('balance_usd')
            .eq('business_id', businessId)
            .single();

        if (!balance) {
            console.warn(`  [Billing] No balance row for ${businessId}`);
            return { ok: false, reason: 'no_balance_row' };
        }

        const newBalance = parseFloat(balance.balance_usd) - amountUsd;
        if (newBalance < 0) {
            return { ok: false, reason: 'insufficient_funds', available: balance.balance_usd };
        }

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

// ==========================================
// 2. ONBOARDING PROGRESS WRITER
// ==========================================
// The frontend polls business_onboarding for progress.
// sync_status JSON gives granular real-time feedback.

async function writeOnboardingProgress(businessId, fields) {
    try {
        await supabase
            .from('business_onboarding')
            .update({ ...fields, updated_at: new Date().toISOString() })
            .eq('business_id', businessId);
    } catch (e) {
        console.error(`  [Onboarding] Progress write failed: ${e.message}`);
    }
}

async function writeSyncStatus(businessId, stage, message, extra = {}) {
    const payload = {
        stage,
        message,
        updated_at: new Date().toISOString(),
        ...extra
    };
    console.log(`  [SyncStatus] ${stage}: ${message}`);
    await writeOnboardingProgress(businessId, { sync_status: payload });
}

// ==========================================
// 3. HELPERS
// ==========================================

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
        return {
            text: m.extendedTextMessage.text,
            type: 'text',
            _contextInfo: m.extendedTextMessage.contextInfo || null
        };
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

// ==========================================
// 4. WHATSAPP BUSINESS PROFILE FETCH
// ==========================================
// Pulls profile from Evolution Go's /chat/fetchProfile endpoint.
// Only fills in blank fields — scraper data takes priority.

async function fetchAndSaveWhatsAppProfile(businessId, instanceToken, jid) {
    try {
        console.log(`  [Profile] Fetching WA Business profile for ${businessId}`);

        const res = await fetch(`${EVOLUTION_URL}/chat/fetchProfile`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'apikey': instanceToken
            },
            body: JSON.stringify({ number: jid })
        });

        if (!res.ok) {
            console.warn(`  [Profile] fetchProfile returned ${res.status}`);
            return;
        }

        const profile = await res.json();
        const data = profile?.data || profile;

        // Only update fields the scraper left blank
        const { data: existing } = await supabase
            .from('businesses')
            .select('name, phone, owner_email, address, description, profile_picture_url, website_url')
            .eq('business_id', businessId)
            .single();

        const updates = {};

        // Name: if current is blank or 'Processing...' or 'My Business'
        const namePlaceholders = ['', 'processing...', 'my business', null, undefined];
        if (namePlaceholders.includes((existing?.name || '').toLowerCase())) {
            const waName = data.name || data.pushName || data.verifiedName;
            if (waName) updates.name = waName;
        }

        // Profile picture: always update (WA is more current than scraper)
        if (data.picture || data.profilePictureUrl) {
            updates.profile_picture_url = data.picture || data.profilePictureUrl;
        }

        // Address: only if blank
        if (!existing?.address && data.address) {
            updates.address = data.address;
        }

        // Description: only if blank
        if (!existing?.description && data.description) {
            updates.description = data.description;
        }

        // Email: only if blank
        if (!existing?.owner_email && data.email) {
            updates.owner_email = data.email;
        }

        // Website: only if blank
        if (!existing?.website_url && data.website) {
            updates.website_url = data.website;
        }

        // Phone: only if blank
        if (!existing?.phone && data.phone) {
            updates.phone = data.phone;
        }

        if (Object.keys(updates).length > 0) {
            await supabase
                .from('businesses')
                .update(updates)
                .eq('business_id', businessId);
            console.log(`  [Profile] ✓ Updated business fields:`, Object.keys(updates).join(', '));
        } else {
            console.log(`  [Profile] All fields already populated — no updates needed`);
        }

    } catch (e) {
        console.error(`  [Profile] fetchAndSaveWhatsAppProfile error: ${e.message}`);
    }
}

// ==========================================
// 5. AD ATTRIBUTION
// ==========================================

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
    const src = (adReply.sourceUrl || '').toLowerCase();
    if (src.includes('instagram')) adPlatform = 'instagram';
    else if (src.includes('facebook') || src.includes('fb.com')) adPlatform = 'facebook';

    return {
        ad_id:            adReply.sourceId        || null,
        ad_headline:      adReply.title            || null,
        ad_body:          adReply.body             || null,
        ad_thumbnail_url: adReply.thumbnailUrl     || null,
        ad_source_url:    adReply.sourceUrl        || null,
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

// ==========================================
// 6. LEAD CLASSIFICATION
// ==========================================

function classifyLeadType(firstMessageText, hasAdAttribution) {
    if (hasAdAttribution) return 'business';
    if (!firstMessageText) return 'unknown';
    const text = firstMessageText.toLowerCase().trim();

    const businessPatterns = [
        /\bprice\b|\bbei\b|\bgharr?ama\b/,
        /\border\b|\bnunua\b|\bniambie\b/,
        /\bavailable\b|\bstock\b|\bipo\b|\bkuna\b/,
        /\bdelivery\b|\bntumie\b/,
        /\bbulk\b|\bwholesale\b|\bjumla\b/,
        /\bquotation\b|\bquote\b|\binvoice\b/,
        /\bproduct\b|\bbidhaa\b/,
        /\bhow much\b|\bnikupatie\b/,
        /\bdo you sell\b|\bdo you have\b|\bmnauza\b/,
    ];
    const personalPatterns = [
        /^(hi|hey|hello|hii|habari|mambo|niaje|sasa|vipi|u good|what'?s up)[\s!?.,]*$/,
        /\bbro\b|\bsis\b|\bdude\b|\bfam\b/,
    ];

    for (const p of businessPatterns) if (p.test(text)) return 'business';
    for (const p of personalPatterns) if (p.test(text)) return 'personal';
    return 'unknown';
}

function extractProductInterests(adData, firstMessageText) {
    const interests = [];
    const sources = [adData?.ad_headline || '', adData?.ad_body || '', firstMessageText || ''].join(' ').toLowerCase();
    if (!sources.trim()) return interests;

    const signals = [
        { pattern: /solar|panel|inverter|battery/, tag: 'solar_energy' },
        { pattern: /rent|apartment|house|plot|land/, tag: 'real_estate' },
        { pattern: /car|vehicle|truck|motorbike/, tag: 'automotive' },
        { pattern: /phone|laptop|computer|tablet/, tag: 'electronics' },
        { pattern: /insurance|cover|policy|bima/, tag: 'insurance' },
        { pattern: /loan|credit|mkopo|finance/, tag: 'financial_services' },
        { pattern: /school|college|course|training/, tag: 'education' },
        { pattern: /clinic|hospital|doctor|dawa/, tag: 'healthcare' },
        { pattern: /food|catering|cake|meal/, tag: 'food_beverage' },
        { pattern: /clothes|dress|fashion|shoes/, tag: 'fashion_apparel' },
        { pattern: /salon|barber|beauty|spa|nails/, tag: 'beauty_wellness' },
        { pattern: /software|app|website|system/, tag: 'software_tech' },
    ];

    for (const { pattern, tag } of signals) {
        if (pattern.test(sources) && !interests.includes(tag)) interests.push(tag);
    }
    return interests;
}

// ==========================================
// 7. CONTACT + CONVERSATION HELPERS
// ==========================================

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
    await supabase.from('contacts')
        .update({ lead_state: 'engaged' })
        .eq('id', contactId);
}

async function cancelPendingFollowUps(contactId) {
    await supabase.from('follow_up_queue')
        .update({ status: 'cancelled', skip_reason: 'lead_replied' })
        .eq('contact_id', contactId)
        .eq('status', 'pending');
}

async function processConsentResponse(contactId, contentText) {
    const text = contentText?.trim().toLowerCase() || '';
    const isStop = /\bstop\b|hapana|usiteme|acha|unsubscribe|opt.?out/i.test(text);
    const isYes  = /\byes\b|\bndio\b|\bok\b|\bsawa\b|subscribe|nipe|tuma/i.test(text);

    if (isStop) {
        await supabase.from('contacts').update({
            do_not_contact:          true,
            follow_up_opted_in:      false,
            follow_up_opted_out_at:  new Date().toISOString()
        }).eq('id', contactId);
        await supabase.from('follow_up_queue').update({
            status: 'cancelled', skip_reason: 'contact_opted_out'
        }).eq('contact_id', contactId).eq('status', 'pending');
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

// ==========================================
// 8. HISTORY SYNC
// ==========================================

async function processHistorySync(payload, businessId, sasaBusinessId) {
    const contacts = payload.data?.contacts || [];
    const chats    = payload.data?.chats    || [];
    const messages = payload.data?.messages || [];

    const stats = { contacts: 0, conversations: 0, messages: 0, skipped: 0, ad_leads: 0, image_products: 0 };
    console.log(`  [History] Raw — contacts:${contacts.length} chats:${chats.length} messages:${messages.length}`);

    await writeSyncStatus(businessId, 'history_sync', `Processing ${contacts.length} contacts and ${messages.length} messages...`);

    // ── Contacts ──────────────────────────────────────────────────
    const validContacts = contacts.filter(c => c.id && !isGroupOrBroadcast(c.id));
    const contactPayloads = validContacts.map(c => ({
        business_id:     businessId,
        social_platform: 'whatsapp',
        social_id:       c.id,
        name:            c.name || c.notify || c.verifiedName || 'Unknown',
        phone:           extractPhone(c.id),
        lead_state:      'new',
        lead_type:       'unknown',
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

    // ── Conversations ─────────────────────────────────────────────
    const validChats = chats.filter(c => c.id && !isGroupOrBroadcast(c.id));
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

    // ── Messages ──────────────────────────────────────────────────
    const cutoffTs        = Math.floor(Date.now() / 1000) - (HISTORY_DAYS * 24 * 60 * 60);
    const convLastMsg     = {};
    const convLastInbound = {};
    const messagePayloads = [];
    const contactFirstMsg = {};
    const outboundImages  = [];

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

        // Collect outbound images for product discovery
        if (isFromMe && content.type === 'image') {
            outboundImages.push(msg);
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

    // Upsert messages in chunks of 100
    for (let i = 0; i < messagePayloads.length; i += 100) {
        const chunk = messagePayloads.slice(i, i + 100);
        const { error } = await supabase
            .from('messages')
            .upsert(chunk, { onConflict: 'whatsapp_message_id' });
        if (!error) stats.messages += chunk.length;
    }
    console.log(`  [History] ${stats.messages} messages upserted, ${stats.skipped} skipped`);

    // ── Classify leads + ad attribution ──────────────────────────
    await writeSyncStatus(businessId, 'classifying_leads', `Classifying ${Object.keys(contactFirstMsg).length} leads...`);

    for (const [contactId, { msg, content }] of Object.entries(contactFirstMsg)) {
        try {
            const adData       = extractAdAttribution(msg);
            const leadType     = classifyLeadType(content.text, !!adData);
            const interests    = extractProductInterests(adData, content.text);
            const contactUpdate = { lead_type: leadType };
            if (interests.length > 0) contactUpdate.product_interests = interests;
            await supabase.from('contacts').update(contactUpdate).eq('id', contactId);
            if (adData) { await recordAdAttribution(businessId, contactId, adData); stats.ad_leads++; }
        } catch (e) {
            console.error(`  [Classify] Error for contact ${contactId}: ${e.message}`);
        }
    }

    // ── Update conversation metadata ──────────────────────────────
    for (const [convId, data] of Object.entries(convLastMsg)) {
        const fields = { last_message_preview: data.preview };
        if (convLastInbound[convId]) {
            fields.last_user_message_at = new Date(convLastInbound[convId] * 1000).toISOString();
        }
        await supabase.from('conversations').update(fields).eq('id', convId);
    }

    // ── Count leads from last 90 days ─────────────────────────────
    const cutoffDate = new Date(Date.now() - HISTORY_DAYS * 24 * 60 * 60 * 1000).toISOString();
    const { count: leadCount } = await supabase
        .from('contacts')
        .select('id', { count: 'exact', head: true })
        .eq('business_id', businessId)
        .gte('created_at', cutoffDate);

    console.log(`  [History] ${leadCount} contacts found in last ${HISTORY_DAYS} days`);

    // ── Write counts to onboarding — show slider, don't charge yet ─
    await writeOnboardingProgress(businessId, {
        leads_processed: leadCount || stats.contacts,  // total found, not yet activated
        sync_status: {
            stage:           'awaiting_lead_activation',
            message:         `Found ${leadCount || stats.contacts} contacts in the last ${HISTORY_DAYS} days. Select how many to activate.`,
            total_found:     leadCount || stats.contacts,
            ad_leads:        stats.ad_leads,
            updated_at:      new Date().toISOString()
        }
    });

    // ── Stage product images for discovery (pending approval) ─────
    if (outboundImages.length > 0) {
        await stageProductImagesForDiscovery(businessId, outboundImages);
    }

    return stats;
}

// ==========================================
// 9. PRODUCT IMAGE STAGING
// ==========================================
// Saves outbound image messages as pending products (is_visible=false).
// User must approve on frontend to trigger the vision AI analysis.

async function stageProductImagesForDiscovery(businessId, imageMessages) {
    try {
        // Deduplicate by thumbnail hash (same logic as System 1)
        const seen = new Set();
        const unique = imageMessages.filter(msg => {
            const thumb = msg.message?.imageMessage?.jpegThumbnail;
            if (!thumb) return false;
            const key = typeof thumb === 'string' ? thumb.substring(0, 50) : JSON.stringify(thumb).substring(0, 50);
            if (seen.has(key)) return false;
            seen.add(key);
            return true;
        });

        console.log(`  [Products] Staging ${unique.length} unique product images for discovery`);

        // Write count to onboarding so frontend can show approval CTA
        await writeOnboardingProgress(businessId, {
            products_pending_approval: unique.length,
            sync_status: {
                stage:      'products_discovered',
                message:    `Found ${unique.length} product images from your WhatsApp history. Approve to analyse them.`,
                count:      unique.length,
                updated_at: new Date().toISOString()
            }
        });

        // Save raw image messages to a staging table for System 1 to process later
        const stagingPayloads = unique.map(msg => ({
            business_id:    businessId,
            raw_payload:    msg,
            media_url:      msg.message?.imageMessage?.url || null,
            thumbnail:      msg.message?.imageMessage?.jpegThumbnail || null,
            caption:        msg.message?.imageMessage?.caption || null,
            status:         'pending_approval',
            created_at:     new Date().toISOString()
        }));

        // Upsert into a product_image_staging table (create this if it doesn't exist)
        await supabase
            .from('product_image_staging')
            .upsert(stagingPayloads, { onConflict: 'business_id, media_url', ignoreDuplicates: true })
            .catch(e => console.warn(`  [Products] Staging upsert warning: ${e.message}`));

    } catch (e) {
        console.error(`  [Products] stageProductImagesForDiscovery error: ${e.message}`);
    }
}

// ==========================================
// 10. LIVE MESSAGE HANDLER
// ==========================================

async function processLiveMessage(payload, businessId) {
    let rawMessages = [];
    if (Array.isArray(payload.data))        rawMessages = payload.data;
    else if (payload.data?.key)             rawMessages = [payload.data];
    else if (payload.data?.messages)        rawMessages = payload.data.messages;
    else { console.log('  [Live] No parseable messages'); return; }

    for (const msg of rawMessages) {
        try {
            const jid = msg.key?.remoteJid;
            if (!jid || isGroupOrBroadcast(jid)) continue;
            if (msg.messageStubType) continue;
            if (!msg.key?.id) continue;

            const isFromMe = msg.key.fromMe === true;
            const pushName = msg.pushName || null;
            const ts       = msg.messageTimestamp || Math.floor(Date.now() / 1000);
            const timestamp = new Date(ts * 1000).toISOString();

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

// ==========================================
// 11. CONNECTION UPDATE HANDLER
// ==========================================

async function processConnectionUpdate(payload, businessId) {
    const state        = payload.data?.state;
    const statusReason = payload.data?.statusReason;
    const instanceToken = new URL(payload.webhookUrl || 'http://x').searchParams.get('sasa_business_id')
        || payload.sasa_business_id
        || null;

    console.log(`  [Connection] ${businessId} → state:${state} reason:${statusReason || 'n/a'}`);

    if (state === 'open') {
        console.log(`  [Connection] ✓ WhatsApp connected for ${businessId}`);

        // ── Write whatsapp_connected to BOTH tables ───────────────
        // businesses.ai_config (for the AI system)
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

        // business_onboarding.whatsapp_connected (for the frontend to advance step)
        await writeOnboardingProgress(businessId, {
            whatsapp_connected: true,
            wa_connected_at:    new Date().toISOString(),
            current_step:       3,   // advance to lead activation step
            sync_status: {
                stage:      'connected',
                message:    'WhatsApp connected. Fetching your business profile and message history...',
                updated_at: new Date().toISOString()
            }
        });

        // ── Fetch WA Business profile (non-blocking) ─────────────
        if (instanceToken) {
            // jid = businessId phone — we read it from the businesses table
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
        console.log(`  [Connection] ✗ WhatsApp disconnected for ${businessId}`);
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

        await writeSyncStatus(businessId, 'disconnected', 'WhatsApp disconnected. Please reconnect from settings.');
    }
}

// ==========================================
// 12. BOT BUILD TRIGGER
// ==========================================
// Calls System 1 (persona pipeline) after lead activation.
// Writes progress to onboarding so frontend can show "Creating your bot..."

async function triggerBotBuild(businessId) {
    try {
        await writeSyncStatus(businessId, 'bot_building', 'Building your AI bot... This takes 1-2 minutes. You can continue setup in the meantime.');

        await supabase.from('businesses')
            .update({ persona_pack_status: 'running' })
            .eq('business_id', businessId);

        const res = await fetch(`${PERSONA_SERVICE_URL}/generate-persona`, {
            method:  'POST',
            headers: { 'Content-Type': 'application/json' },
            body:    JSON.stringify({ business_id: businessId })
        });

        if (res.ok) {
            console.log(`  [BotBuild] ✓ Persona pipeline triggered for ${businessId}`);
            // Status moves to 'ready' when System 1 finishes — it calls update_persona_status('ready')
            // which the frontend polls via /persona-status/{businessId}
        } else if (res.status === 402) {
            const body = await res.json();
            console.warn(`  [BotBuild] Insufficient funds for ${businessId}: ${body.message}`);
            await writeSyncStatus(businessId, 'bot_build_failed', 'Insufficient credits to build AI bot. Please top up.');
            await supabase.from('businesses')
                .update({ persona_pack_status: 'insufficient_funds' })
                .eq('business_id', businessId);
        } else {
            console.error(`  [BotBuild] Persona service returned ${res.status}`);
            await writeSyncStatus(businessId, 'bot_build_failed', 'Bot build failed. Will retry automatically.');
            await supabase.from('businesses')
                .update({ persona_pack_status: 'failed' })
                .eq('business_id', businessId);
        }
    } catch (e) {
        console.error(`  [BotBuild] triggerBotBuild error: ${e.message}`);
        await writeSyncStatus(businessId, 'bot_build_failed', 'Bot build failed — network error. Will retry.');
    }
}

// ==========================================
// 13. EXPRESS ROUTES
// ==========================================

// ── Webhook from Evolution Go ─────────────────────────────────
app.post('/webhook/evolution', async (req, res) => {
    const businessId    = req.query.business_id;
    const sasaBusinessId = req.query.sasa_business_id;
    const eventType     = req.body?.event;

    if (!businessId) return res.status(400).json({ error: 'Missing business_id query param' });

    res.status(200).send('OK');  // Always ack immediately

    console.log(`\n[Webhook] event:${eventType} | business:${businessId}`);

    try {
        switch (eventType) {
            case 'messaging-history.set':
                await processHistorySync(req.body, businessId, sasaBusinessId);
                console.log(`[History] ✓ Sync complete for ${businessId}`);
                break;

            case 'messages.upsert':
                await processLiveMessage(req.body, businessId);
                break;

            case 'connection.update':
                // Inject sasa_business_id so connection handler can use it for profile fetch
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

// ── Lead activation (called after user confirms slider) ───────
// Request: { business_id, count }
// 1. Checks balance for count × lead_ingestion_min_kes
// 2. Deducts credits
// 3. Marks leads_processed = count in onboarding
// 4. Advances current_step to 4
// 5. Triggers bot build
app.post('/leads/activate', async (req, res) => {
    const { business_id, count } = req.body;
    if (!business_id)    return res.status(400).json({ error: 'business_id required' });
    if (!count || count < 1) return res.status(400).json({ error: 'count must be >= 1' });

    try {
        const pricePerLead = await getBillingValue('lead_ingestion_min_kes', 0.75);
        const totalKes     = count * pricePerLead;

        // Check + deduct balance
        const charge = await chargeKes(business_id, totalKes, `Lead activation — ${count} leads @ ${pricePerLead} KES each`);
        if (!charge.ok) {
            return res.status(402).json({
                error:          'insufficient_funds',
                required_kes:   totalKes,
                reason:         charge.reason
            });
        }

        // Mark leads activated in onboarding, advance step
        await writeOnboardingProgress(business_id, {
            leads_processed: count,
            leads_done_at:   new Date().toISOString(),
            current_step:    4,
            sync_status: {
                stage:      'leads_activated',
                message:    `${count} leads activated. Your AI bot is now being built...`,
                count,
                charged_kes: totalKes,
                updated_at:  new Date().toISOString()
            }
        });

        res.json({
            ok:          true,
            activated:   count,
            charged_kes: totalKes,
            charged_usd: charge.charged_usd
        });

        // Trigger bot build in background (non-blocking)
        triggerBotBuild(business_id).catch(e => console.error('[LeadActivate] Bot build error:', e.message));

    } catch (e) {
        console.error('[LeadActivate] Error:', e.message);
        res.status(500).json({ error: e.message });
    }
});

// ── Product image approval (called when user approves discovery) ─
// Request: { business_id }
// Sets product images to approved state, marks products_seeded = true,
// then the System 1 image harvester will pick them up on next persona run.
app.post('/products/approve-discovery', async (req, res) => {
    const { business_id } = req.body;
    if (!business_id) return res.status(400).json({ error: 'business_id required' });

    try {
        // Mark staged images as approved
        const { data: approved, error } = await supabase
            .from('product_image_staging')
            .update({ status: 'approved', approved_at: new Date().toISOString() })
            .eq('business_id', business_id)
            .eq('status', 'pending_approval')
            .select('id');

        if (error) throw error;

        const approvedCount = approved?.length || 0;
        console.log(`  [Products] ${approvedCount} images approved for ${business_id}`);

        // Mark products step done in onboarding
        await writeOnboardingProgress(business_id, {
            products_seeded:    true,
            products_done_at:   new Date().toISOString(),
            products_pending_approval: 0,
            sync_status: {
                stage:      'products_approved',
                message:    `${approvedCount} product images approved. They will be analysed and added to your catalog shortly.`,
                count:      approvedCount,
                updated_at: new Date().toISOString()
            }
        });

        res.json({ ok: true, approved_count: approvedCount });

    } catch (e) {
        console.error('[ProductApproval] Error:', e.message);
        res.status(500).json({ error: e.message });
    }
});

// ── Health check ──────────────────────────────────────────────
app.get('/health', (_req, res) => res.json({ status: 'ok', service: 'whatsapp-cleaner-v3' }));

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`WhatsApp cleaner v3 online — port ${PORT}`));
