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

const HISTORY_DAYS         = parseInt(process.env.HISTORY_DAYS || '90');
const LEAPCELL_PERSONA_URL = process.env.LEAPCELL_PERSONA_URL || null;

// ==========================================
// 1. HELPERS
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
            // Pass context through for ad attribution extraction
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
            text:      `Location: ${m.locationMessage.degreesLatitude}, ${m.locationMessage.degreesLongitude}`,
            type:      'location',
            latitude:  m.locationMessage.degreesLatitude,
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
// 2. AD ATTRIBUTION EXTRACTION
// ==========================================

/**
 * Extracts Click-to-WhatsApp ad attribution from a message.
 *
 * Meta passes this in contextInfo.externalAdReply when a lead
 * clicks a CTWA (Click-to-WhatsApp) ad on Facebook or Instagram.
 *
 * Returns null if this is not an ad-originated message.
 */
function extractAdAttribution(msg) {
    const m = msg.message;
    if (!m) return null;

    // CTWA ads land as extendedTextMessage with contextInfo.externalAdReply
    const contextInfo =
        m.extendedTextMessage?.contextInfo ||
        m.imageMessage?.contextInfo        ||
        m.videoMessage?.contextInfo        ||
        null;

    const adReply = contextInfo?.externalAdReply;
    if (!adReply) return null;

    // Determine source platform from sourceUrl
    let adPlatform = 'meta';
    const src = (adReply.sourceUrl || '').toLowerCase();
    if (src.includes('instagram')) adPlatform = 'instagram';
    else if (src.includes('facebook') || src.includes('fb.com')) adPlatform = 'facebook';

    return {
        ad_id:            adReply.sourceId        || null,
        ad_headline:      adReply.title            || null,
        ad_body:          adReply.body             || null,
        ad_thumbnail_url: adReply.thumbnailUrl     || null,
        ad_media_url:     adReply.mediaUrl         || null,
        ad_source_url:    adReply.sourceUrl        || null,
        ad_media_type:    adReply.mediaType        || null,   // IMAGE, VIDEO, etc.
        ad_platform:      adPlatform,
        ad_render_large:  adReply.renderLargerThumbnail || false,
        captured_at:      new Date().toISOString()
    };
}

/**
 * Upserts ad attribution into the ad_attributions table and
 * increments the lead count for this ad.
 * Returns the attribution record id.
 */
async function recordAdAttribution(businessId, contactId, adData) {
    if (!adData?.ad_id) return null;

    try {
        // Upsert the ad definition itself (one row per ad_id per business)
        const { data: adRecord, error: adErr } = await supabase
            .from('ad_attributions')
            .upsert({
                business_id: businessId,
                ad_id:            adData.ad_id,
                ad_headline:      adData.ad_headline,
                ad_body:          adData.ad_body,
                ad_thumbnail_url: adData.ad_thumbnail_url,
                ad_media_url:     adData.ad_media_url,
                ad_source_url:    adData.ad_source_url,
                ad_media_type:    adData.ad_media_type,
                ad_platform:      adData.ad_platform,
                // Increment lead_count each time a new contact comes from this ad
                // The DB should handle this with a trigger or we use RPC
            }, {
                onConflict: 'business_id, ad_id',
                ignoreDuplicates: false
            })
            .select('id')
            .single();

        if (adErr) {
            console.error(`  [Ad] Upsert failed: ${adErr.message}`);
            return null;
        }

        // Increment lead count for this ad
        await supabase.rpc('increment_ad_lead_count', {
            p_ad_id:          adRecord.id,
            p_business_id: businessId
        }).catch(e => console.warn(`  [Ad] Lead count RPC failed: ${e.message}`));

        // Tag the contact with this ad attribution
        // Note: original_ad_id is the existing column on contacts for the raw Meta ad_id
        await supabase
            .from('contacts')
            .update({
                is_ad_lead:          true,
                lead_type:           'business',   // Ad leads are always business leads
                ad_attribution_id:   adRecord.id,
                original_ad_id:      adData.ad_id,
                ad_headline:         adData.ad_headline,
                ad_body:             adData.ad_body,
                ad_thumbnail_url:    adData.ad_thumbnail_url,
                ad_platform:         adData.ad_platform,
                ad_attributed_at:    adData.captured_at
            })
            .eq('id', contactId);

        console.log(`  [Ad] ✓ Attribution recorded — ad_id:${adData.ad_id} → contact:${contactId}`);
        return adRecord.id;

    } catch (e) {
        console.error(`  [Ad] recordAdAttribution error: ${e.message}`);
        return null;
    }
}

// ==========================================
// 3. LEAD CLASSIFICATION
// ==========================================

/**
 * Classifies a lead as 'personal' or 'business' based on available signals.
 *
 * Rules (in priority order):
 * 1. If we have ad attribution → always 'business' (ads only target buyers/prospects)
 * 2. If first message contains strong business signals → 'business'
 * 3. If first message contains personal/social signals → 'personal'
 * 4. Otherwise → 'unknown' (AI will refine later during conversation analysis)
 *
 * Business signals: price inquiry, product names, bulk/wholesale, "bei", "price",
 *   "order", "delivery", "available", "stock", "quotation", "invoice", "supply"
 *
 * Personal signals: first-name-only greetings, "hi bro/sis", "niko sawa",
 *   purely social Swahili openers with no product context
 */
function classifyLeadType(firstMessageText, hasAdAttribution) {
    // Ad leads are definitionally business leads
    if (hasAdAttribution) return 'business';

    if (!firstMessageText) return 'unknown';

    const text = firstMessageText.toLowerCase().trim();

    // Strong business intent signals (English + Swahili)
    const businessPatterns = [
        /\bprice\b|\bbei\b|\bgharr?ama\b/,
        /\border\b|\bnunua\b|\bniambie\b/,
        /\bavailable\b|\bstock\b|\bipo\b|\bkuna\b/,
        /\bdelivery\b|\bdelivering\b|\bdelivar\b|\bntumie\b/,
        /\bbulk\b|\bwholesale\b|\bjumla\b/,
        /\bquotation\b|\bquote\b|\binvoice\b/,
        /\bproduct\b|\bbidhaa\b/,
        /\bsupply\b|\bsupplier\b/,
        /\bhow much\b|\bnikupatie\b|\bnitumie\b/,
        /\bdo you sell\b|\bdo you have\b|\bmnauza\b/,
    ];

    // Strong personal / social signals
    const personalPatterns = [
        /^(hi|hey|hello|hii|habari|mambo|niaje|sasa|vipi|uko?|u good|what'?s up)[\s!?.,]*$/,
        /\bbro\b|\bsis\b|\bdude\b|\bfam\b/,
        /\bniko sawa\b|\bnzuri\b.*\bnini\b/,
    ];

    for (const pattern of businessPatterns) {
        if (pattern.test(text)) return 'business';
    }

    for (const pattern of personalPatterns) {
        if (pattern.test(text)) return 'personal';
    }

    return 'unknown';
}

/**
 * Extracts product interest signals from ad creative + first message.
 * Returns an array of product interest tags.
 *
 * This seeds the AI persona pack with concrete product context per lead.
 */
function extractProductInterests(adData, firstMessageText) {
    const interests = [];
    const sources   = [
        adData?.ad_headline || '',
        adData?.ad_body     || '',
        firstMessageText    || ''
    ].join(' ').toLowerCase();

    if (!sources.trim()) return interests;

    // These are generic signal patterns — the AI enrichment step will
    // do deep NLP extraction; this is the cheap first pass for tagging
    const productSignals = [
        { pattern: /solar|panel|inverter|battery|off.?grid/,  tag: 'solar_energy' },
        { pattern: /rent|apartment|house|plot|land|bedsit/,   tag: 'real_estate' },
        { pattern: /car|vehicle|truck|motorbike|tuk.?tuk/,    tag: 'automotive' },
        { pattern: /phone|laptop|computer|tablet|gadget/,     tag: 'electronics' },
        { pattern: /insurance|cover|policy|bima/,             tag: 'insurance' },
        { pattern: /loan|credit|borrow|mkopo|finance/,        tag: 'financial_services' },
        { pattern: /school|college|course|training|admission/, tag: 'education' },
        { pattern: /clinic|hospital|doctor|dawa|medicine/,    tag: 'healthcare' },
        { pattern: /food|catering|cake|meal|deliver.*food/,   tag: 'food_beverage' },
        { pattern: /clothes|dress|fashion|shoes|outfit/,      tag: 'fashion_apparel' },
        { pattern: /salon|barber|beauty|spa|nails/,           tag: 'beauty_wellness' },
        { pattern: /hotel|airbnb|accommodation|lodge|resort/,  tag: 'hospitality' },
        { pattern: /software|app|website|system|tech/,        tag: 'software_tech' },
        { pattern: /gym|fitness|yoga|workout/,                tag: 'fitness' },
        { pattern: /printing|branding|design|logo|marketing/, tag: 'marketing_services' },
    ];

    for (const { pattern, tag } of productSignals) {
        if (pattern.test(sources) && !interests.includes(tag)) {
            interests.push(tag);
        }
    }

    return interests;
}

// ==========================================
// 4. SYSTEM 1 TRIGGER
// ==========================================

async function triggerPersonaPackGeneration(businessId) {
    if (!LEAPCELL_PERSONA_URL) {
        console.log(`  [System1] LEAPCELL_PERSONA_URL not set — skipping for ${businessId}`);
        return;
    }
    try {
        const res = await fetch(LEAPCELL_PERSONA_URL, {
            method:  'POST',
            headers: { 'Content-Type': 'application/json' },
            body:    JSON.stringify({ business_id: businessId })
        });
        if (res.ok) {
            console.log(`  [System1] ✓ Persona pack generation triggered for ${businessId}`);
        } else {
            console.error(`  [System1] ✗ HTTP ${res.status}`);
        }
    } catch (e) {
        console.error(`  [System1] ✗ ${e.message}`);
    }
}

// ==========================================
// 5. CONTACT + CONVERSATION HELPERS
// ==========================================

/**
 * Finds an existing contact by WhatsApp JID or creates a new one.
 * Only updates name if we have a pushName and the current name is blank/Unknown.
 * Returns { id, lead_state, name, is_ad_lead, lead_type }
 */
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
        if (pushName && (!existing.name || existing.name === 'Unknown')) {
            updates.name = pushName;
        }
        await supabase.from('contacts').update(updates).eq('id', existing.id);
        return existing;
    }

    const { data: created, error } = await supabase
        .from('contacts')
        .insert({
            business_id: businessId,
            social_platform:  'whatsapp',
            social_id:        jid,
            name:             pushName || 'Unknown',
            phone:            phone,
            lead_state:       'new',
            lead_type:        'unknown',   // Will be classified on first message
            is_ad_lead:       false,
            follow_up_count:  0,
            last_seen:        new Date().toISOString()
        })
        .select('id, lead_state, name, is_ad_lead, lead_type, original_ad_id, ad_attribution_id')
        .single();

    if (error) throw new Error(`Contact create failed: ${error.message}`);
    console.log(`  [Contact] New contact created: ${created.id} (${pushName || jid})`);
    return created;
}

/**
 * Finds an existing conversation or creates one.
 * Returns the conversation id.
 */
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
            business_id: businessId,
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
    console.log(`  [Conversation] New conversation created: ${created.id}`);
    return created.id;
}

/**
 * Lead state machine — transitions to 'engaged' on inbound reply.
 */
async function updateLeadStateOnReply(contactId, currentState) {
    const transitionStates = ['new', 'stalled', 'ghosted', 'warm'];
    if (!transitionStates.includes(currentState)) return;

    const { error } = await supabase
        .from('contacts')
        .update({ lead_state: 'engaged' })
        .eq('id', contactId);

    if (error) {
        console.error(`  [State] Lead state update failed: ${error.message}`);
    } else {
        console.log(`  [State] ${contactId}: ${currentState} → engaged`);
    }
}

/**
 * Cancels all pending follow-ups when a lead replies.
 */
async function cancelPendingFollowUps(contactId) {
    const { data, error } = await supabase
        .from('follow_up_queue')
        .update({ status: 'cancelled', skip_reason: 'lead_replied' })
        .eq('contact_id', contactId)
        .eq('status', 'pending')
        .select('id');

    if (error) {
        console.error(`  [Queue] Cancel follow-ups failed: ${error.message}`);
    } else if (data?.length > 0) {
        console.log(`  [Queue] ${data.length} pending follow-up(s) cancelled for contact ${contactId}`);
    }
}

/**
 * Consent response handler — STOP / YES detection in EN + SW.
 */
async function processConsentResponse(contactId, contentText) {
    const text = contentText?.trim().toLowerCase() || '';

    const isStop = /\bstop\b|hapana|usiteme|acha|unsubscribe|opt.?out/i.test(text);
    const isYes  = /\byes\b|\bndio\b|\bok\b|\bsawa\b|\bokay\b|subscribe|nipe|tuma/i.test(text);

    if (isStop) {
        await supabase.from('contacts').update({
            do_not_contact:        true,
            follow_up_opted_in:    false,
            follow_up_opted_out_at: new Date().toISOString()
        }).eq('id', contactId);

        await supabase.from('follow_up_queue').update({
            status:      'cancelled',
            skip_reason: 'contact_opted_out'
        }).eq('contact_id', contactId).eq('status', 'pending');

        console.log(`  [Consent] STOP received — contact ${contactId} permanently removed`);
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

            console.log(`  [Consent] YES received — contact ${contactId} opted in`);
        }
    }
}

// ==========================================
// 6. AD QUALITY SCORING
// ==========================================

/**
 * Updates the quality score for an ad based on the quality of leads it generated.
 *
 * Quality signals we track per ad:
 * - reply_rate: % of ad leads who replied (vs just clicked and ghosted)
 * - engagement_depth: avg messages exchanged
 * - conversion_rate: % who became customers (lead_state = 'won')
 * - product_interest_rate: % who asked about a specific product
 *
 * This runs async after lead classification — not on the critical path.
 */
async function updateAdQualitySignals(businessId, adAttributionId, signalType) {
    if (!adAttributionId) return;

    try {
        const fieldMap = {
            replied:          'reply_count',
            product_interest: 'product_interest_count',
            converted:        'conversion_count',
        };

        const field = fieldMap[signalType];
        if (!field) return;

        await supabase.rpc('increment_ad_quality_signal', {
            p_ad_attribution_id: adAttributionId,
            p_field:             field
        }).catch(e => console.warn(`  [AdQuality] RPC failed: ${e.message}`));

    } catch (e) {
        console.error(`  [AdQuality] Error: ${e.message}`);
    }
}

// ==========================================
// 7. HISTORY SYNC
// ==========================================

async function processHistorySync(payload, businessId) {
    const contacts = payload.data?.contacts || [];
    const chats    = payload.data?.chats    || [];
    const messages = payload.data?.messages || [];

    const stats = { contacts: 0, conversations: 0, messages: 0, skipped: 0, ad_leads: 0 };
    console.log(`  Raw — contacts:${contacts.length} chats:${chats.length} messages:${messages.length}`);

    // ── Contacts ─────────────────────────────────────────────────
    const validContacts   = contacts.filter(c => c.id && !isGroupOrBroadcast(c.id));
    const contactPayloads = validContacts.map(c => ({
        business_id: businessId,
        social_platform:  'whatsapp',
        social_id:        c.id,
        name:             c.name || c.notify || c.verifiedName || 'Unknown',
        phone:            extractPhone(c.id),
        lead_state:       'new',
        lead_type:        'unknown',
        is_ad_lead:       false
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
        console.log(`  [Contacts] ${stats.contacts} upserted`);
    }

    // ── Conversations ────────────────────────────────────────────
    const validChats           = chats.filter(c => c.id && !isGroupOrBroadcast(c.id));
    const conversationPayloads = validChats
        .map(chat => ({
            business_id: businessId,
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
        console.log(`  [Conversations] ${stats.conversations} upserted`);
    }

    // ── Messages + Ad Attribution ─────────────────────────────────
    const cutoffTs        = Math.floor(Date.now() / 1000) - (HISTORY_DAYS * 24 * 60 * 60);
    const convLastMsg     = {};
    const convLastInbound = {};
    const messagePayloads = [];

    // Track first inbound message per contact for classification
    const contactFirstMsg = {};

    for (const msg of messages) {
        const jid = msg.key?.remoteJid;
        if (!jid || isGroupOrBroadcast(jid))        { stats.skipped++; continue; }
        if ((msg.messageTimestamp || 0) < cutoffTs) { stats.skipped++; continue; }
        if (msg.messageStubType)                     { stats.skipped++; continue; }

        const conversationId = convoMap[jid];
        const contactId      = contactMap[jid] || null;
        if (!conversationId)                         { stats.skipped++; continue; }

        const content = extractMessageContent(msg);
        if (!content)                                { stats.skipped++; continue; }

        const isFromMe = msg.key.fromMe === true;
        const ts       = msg.messageTimestamp;

        if (!convLastMsg[conversationId] || ts > convLastMsg[conversationId].ts) {
            convLastMsg[conversationId] = {
                ts,
                preview: (content.text || content.type || '').slice(0, 120)
            };
        }
        if (!isFromMe) {
            if (!convLastInbound[conversationId] || ts > convLastInbound[conversationId]) {
                convLastInbound[conversationId] = ts;
            }
            // Track first inbound message per contact (lowest timestamp)
            if (!contactFirstMsg[contactId] || ts < contactFirstMsg[contactId].ts) {
                contactFirstMsg[contactId] = { ts, msg, content };
            }
        }

        messagePayloads.push({
            whatsapp_message_id: msg.key.id,
            business_id:    businessId,
            contact_id:          contactId,
            conversation_id:     conversationId,
            direction:           isFromMe ? 'out' : 'in',
            role:                isFromMe ? 'admin' : 'user',
            agent_role:          isFromMe ? 'human' : 'legacy_ai',
            type:                content.type,
            content:             content,
            created_at:          new Date(ts * 1000).toISOString(),
            status:              'sent',
            is_read:             isFromMe,
            raw_payload:         msg
        });
    }

    // Upsert messages in chunks
    for (let i = 0; i < messagePayloads.length; i += 100) {
        const chunk = messagePayloads.slice(i, i + 100);
        const { error } = await supabase
            .from('messages')
            .upsert(chunk, { onConflict: 'whatsapp_message_id' });
        if (error) {
            console.error(`  [Messages] Chunk ${Math.floor(i / 100) + 1} error: ${error.message}`);
        } else {
            stats.messages += chunk.length;
        }
    }
    console.log(`  [Messages] ${stats.messages} upserted, ${stats.skipped} skipped`);

    // ── Post-sync: classify leads + ad attribution ────────────────
    for (const [contactId, { msg, content }] of Object.entries(contactFirstMsg)) {
        try {
            const adData = extractAdAttribution(msg);

            // Lead classification
            const leadType = classifyLeadType(content.text, !!adData);
            const productInterests = extractProductInterests(adData, content.text);

            const contactUpdate = {
                lead_type: leadType,
                ...(productInterests.length > 0 ? { product_interests: productInterests } : {})
            };
            await supabase.from('contacts').update(contactUpdate).eq('id', contactId);

            // Ad attribution
            if (adData) {
                await recordAdAttribution(businessId, contactId, adData);
                stats.ad_leads++;
            }
        } catch (e) {
            console.error(`  [Classify] Error for contact ${contactId}: ${e.message}`);
        }
    }
    console.log(`  [Classification] ${Object.keys(contactFirstMsg).length} contacts classified, ${stats.ad_leads} ad leads found`);

    // ── Update conversation metadata ──────────────────────────────
    let metaUpdated = 0;
    for (const [convId, data] of Object.entries(convLastMsg)) {
        const fields = { last_message_preview: data.preview };
        if (convLastInbound[convId]) {
            fields.last_user_message_at = new Date(convLastInbound[convId] * 1000).toISOString();
        }
        const { error } = await supabase.from('conversations').update(fields).eq('id', convId);
        if (!error) metaUpdated++;
    }
    console.log(`  [Conversations] ${metaUpdated} metadata rows updated`);

    return stats;
}

// ==========================================
// 8. LIVE MESSAGE HANDLER
// ==========================================

async function processLiveMessage(payload, businessId) {
    let rawMessages = [];
    if (Array.isArray(payload.data)) {
        rawMessages = payload.data;
    } else if (payload.data?.key) {
        rawMessages = [payload.data];
    } else if (payload.data?.messages) {
        rawMessages = payload.data.messages;
    } else {
        console.log('  [Live] No parseable messages in payload');
        return;
    }

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

            // ── Get or create contact + conversation ─────────────
            const contact        = await getOrCreateContact(businessId, jid, pushName);
            const contactId      = contact.id;
            const conversationId = await getOrCreateConversation(businessId, contactId, jid);

            // ── Extract content ──────────────────────────────────
            const content = extractMessageContent(msg);
            if (!content) {
                console.log(`  [Live] Unhandled message type from ${jid} — skipping`);
                continue;
            }

            // ── Ad attribution (inbound only, first message only) ─
            let adAttributionId = null;
            if (!isFromMe) {
                const adData = extractAdAttribution(msg);

                if (adData && !contact.is_ad_lead) {
                    // This is the first (or only) ad-attributed message — record it
                    adAttributionId = await recordAdAttribution(businessId, contactId, adData);
                } else if (contact.ad_attribution_id) {
                    adAttributionId = contact.ad_attribution_id;
                }

                // Classify lead type if still unknown
                if (!contact.lead_type || contact.lead_type === 'unknown') {
                    const adData2 = adData || (contact.is_ad_lead ? { ad_id: contact.original_ad_id } : null);
                    const leadType = classifyLeadType(content.text, !!adData2);
                    const productInterests = extractProductInterests(adData2, content.text);

                    const contactUpdate = { lead_type: leadType };
                    if (productInterests.length > 0) contactUpdate.product_interests = productInterests;

                    await supabase.from('contacts').update(contactUpdate).eq('id', contactId);

                    // Signal quality: this lead engaged
                    if (adAttributionId) {
                        await updateAdQualitySignals(businessId, adAttributionId, 'replied');

                        if (productInterests.length > 0) {
                            await updateAdQualitySignals(businessId, adAttributionId, 'product_interest');
                        }
                    }
                }
            }

            // ── Insert message ───────────────────────────────────
            const { error: msgError } = await supabase
                .from('messages')
                .upsert({
                    whatsapp_message_id: msg.key.id,
                    business_id:    businessId,
                    contact_id:          contactId,
                    conversation_id:     conversationId,
                    direction:           isFromMe ? 'out' : 'in',
                    role:                isFromMe ? 'admin' : 'user',
                    agent_role:          isFromMe ? 'human' : 'legacy_ai',
                    type:                content.type,
                    content:             content,
                    created_at:          timestamp,
                    status:              'sent',
                    is_read:             isFromMe,
                    raw_payload:         msg
                }, { onConflict: 'whatsapp_message_id' });

            if (msgError) {
                console.error(`  [Live] Message insert error: ${msgError.message}`);
                continue;
            }

            // ── Update conversation metadata ─────────────────────
            const preview    = (content.text || content.type || '').slice(0, 120);
            const convUpdate = { last_message_preview: preview, status: 'open' };
            if (!isFromMe) convUpdate.last_user_message_at = timestamp;

            await supabase.from('conversations').update(convUpdate).eq('id', conversationId);

            // ── Inbound-only: state machine + queue cleanup ──────
            if (!isFromMe) {
                await updateLeadStateOnReply(contactId, contact.lead_state);
                await cancelPendingFollowUps(contactId);
                await processConsentResponse(contactId, content.text);
                // AI routing goes here in Phase 4
            }

            console.log(
                `  [Live] ✓ ${isFromMe ? 'OUT' : 'IN '} | ` +
                `type:${content.type.padEnd(12)} | ` +
                `contact:${contactId} | ` +
                `conv:${conversationId}` +
                (adAttributionId ? ` | ad:${adAttributionId}` : '')
            );

        } catch (msgErr) {
            console.error(`  [Live] Error processing message: ${msgErr.message}`);
        }
    }
}

// ==========================================
// 9. CONNECTION UPDATE HANDLER
// ==========================================

async function processConnectionUpdate(payload, businessId) {
    const state        = payload.data?.state;
    const statusReason = payload.data?.statusReason;

    console.log(`  [Connection] ${businessId} → state: ${state} (reason: ${statusReason || 'n/a'})`);

    if (state === 'open') {
        console.log(`  [Connection] ✓ WhatsApp connected for ${businessId}`);
        try {
            const { data: biz } = await supabase
                .from('businesses')
                .select('ai_config')
                .eq('business_id', businessId)
                .single();

            await supabase
                .from('businesses')
                .update({
                    ai_config: {
                        ...(biz?.ai_config || {}),
                        whatsapp_status:       'connected',
                        whatsapp_connected_at: new Date().toISOString()
                    }
                })
                .eq('business_id', businessId);

        } catch (e) {
            console.error(`  [Connection] Failed to update ai_config: ${e.message}`);
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

            await supabase
                .from('businesses')
                .update({
                    ai_config: {
                        ...(biz?.ai_config || {}),
                        whatsapp_status:          'disconnected',
                        whatsapp_disconnected_at: new Date().toISOString()
                    }
                })
                .eq('business_id', businessId);

        } catch (e) {
            console.error(`  [Connection] Failed to update ai_config: ${e.message}`);
        }
    }
}

// ==========================================
// 10. EXPRESS ROUTES
// ==========================================

app.post('/webhook/evolution', async (req, res) => {
    const businessId = req.query.business_id;
    const eventType  = req.body?.event;

    if (!businessId) {
        return res.status(400).json({ error: 'Missing business_id query param' });
    }

    res.status(200).send('OK');

    console.log(`\n[Webhook] event:${eventType} | business:${businessId}`);

    try {
        switch (eventType) {

            case 'messaging-history.set':
                await processHistorySync(req.body, businessId);
                console.log(`[History] ✓ Sync complete for ${businessId}`);
                await triggerPersonaPackGeneration(businessId);
                break;

            case 'messages.upsert':
                await processLiveMessage(req.body, businessId);
                break;

            case 'connection.update':
                await processConnectionUpdate(req.body, businessId);
                break;

            default:
                console.log(`[Webhook] Unhandled event type: ${eventType}`);
        }
    } catch (err) {
        console.error(`[Webhook] Error handling ${eventType} for ${businessId}: ${err.message}`);
    }
});

app.get('/health', (_req, res) => res.json({ status: 'ok', service: 'whatsapp-cleaner-v2' }));

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`WhatsApp cleaner v2 online — port ${PORT}`));
