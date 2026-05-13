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

const HISTORY_DAYS        = parseInt(process.env.HISTORY_DAYS || '90');
const LEAPCELL_PERSONA_URL = process.env.LEAPCELL_PERSONA_URL || null;

// ==========================================
// 1. HELPERS
// ==========================================

/**
 * Returns true for group chats, broadcast lists, and status feeds.
 * These must be filtered before any DB writes.
 */
function isGroupOrBroadcast(jid) {
    if (!jid) return true;
    return (
        jid.endsWith('@g.us')        ||
        jid.endsWith('@broadcast')   ||
        jid === 'status@broadcast'   ||
        jid.includes('newsletter')   ||
        jid.includes('lid')           // WhatsApp linked device phantom JIDs
    );
}

/**
 * Pulls the phone number out of a WhatsApp JID.
 * 254712345678@s.whatsapp.net  →  +254712345678
 */
function extractPhone(jid) {
    if (!jid) return null;
    const raw = jid.split('@')[0].replace(/\D/g, '');
    if (!raw || raw.length < 7) return null;
    return `+${raw}`;
}

/**
 * Extracts content and type from any WhatsApp message object.
 * Returns { text, type, ...extras } or null for unhandled/empty messages.
 */
function extractMessageContent(msg) {
    const m = msg.message;
    if (!m) return null;

    // Plain text
    if (m.conversation) {
        return { text: m.conversation, type: 'text' };
    }

    // Extended text (link previews, quoted replies)
    if (m.extendedTextMessage?.text) {
        return { text: m.extendedTextMessage.text, type: 'text' };
    }

    // Image
    if (m.imageMessage) {
        return {
            text:    m.imageMessage.caption || '',
            type:    'image',
            caption: m.imageMessage.caption || ''
        };
    }

    // Voice note (ptt = push-to-talk = voice note)
    if (m.audioMessage) {
        return {
            text:             '',
            type:             m.audioMessage.ptt ? 'voice_note' : 'audio',
            duration_seconds: m.audioMessage.seconds || null
        };
    }

    // Video
    if (m.videoMessage) {
        return {
            text:    m.videoMessage.caption || '',
            type:    'video',
            caption: m.videoMessage.caption || ''
        };
    }

    // Document / file
    if (m.documentMessage) {
        return {
            text:      m.documentMessage.fileName || '',
            type:      'document',
            file_name: m.documentMessage.fileName || ''
        };
    }

    // Sticker
    if (m.stickerMessage) {
        return { text: '', type: 'sticker' };
    }

    // Reaction (emoji on a message)
    if (m.reactionMessage) {
        return {
            text:  m.reactionMessage.text || '',
            type:  'reaction',
            emoji: m.reactionMessage.text || ''
        };
    }

    // Location share
    if (m.locationMessage) {
        const lat = m.locationMessage.degreesLatitude;
        const lng = m.locationMessage.degreesLongitude;
        return {
            text:      `Location shared: ${lat}, ${lng}`,
            type:      'location',
            latitude:  lat,
            longitude: lng
        };
    }

    // Contact card
    if (m.contactMessage) {
        return {
            text: m.contactMessage.displayName || 'Contact shared',
            type: 'contact'
        };
    }

    // Button response
    if (m.buttonsResponseMessage) {
        return {
            text: m.buttonsResponseMessage.selectedDisplayText || '',
            type: 'button_response'
        };
    }

    // List response
    if (m.listResponseMessage) {
        return {
            text: m.listResponseMessage.title || '',
            type: 'list_response'
        };
    }

    // Anything else we can't handle meaningfully
    return null;
}

// ==========================================
// 2. SYSTEM 1 TRIGGER
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
            console.log(`  [System1] ✓ Persona pack generation triggered`);
        } else {
            console.error(`  [System1] ✗ HTTP ${res.status}`);
        }
    } catch (e) {
        console.error(`  [System1] ✗ Fetch error: ${e.message}`);
    }
}

// ==========================================
// 3. CORE SYNC FUNCTION
// ==========================================
async function processHistorySync(payload, businessId) {
    const contacts = payload.data?.contacts || [];
    const chats    = payload.data?.chats    || [];
    const messages = payload.data?.messages || [];

    const stats = { contacts: 0, conversations: 0, messages: 0, skipped: 0 };
    console.log(`  Raw payload — contacts:${contacts.length} chats:${chats.length} messages:${messages.length}`);

    // ── 1. CONTACTS ─────────────────────────────────────────────
    const validContacts = contacts.filter(c => c.id && !isGroupOrBroadcast(c.id));

    const contactPayloads = validContacts.map(c => ({
        business_id:     businessId,
        social_platform: 'whatsapp',
        social_id:       c.id,
        name:            c.name || c.notify || c.verifiedName || 'Unknown',
        phone:           extractPhone(c.id),
        lead_state:      'new'
    }));

    let contactMap = {};

    if (contactPayloads.length > 0) {
        const { data: insertedContacts, error: contactError } = await supabase
            .from('contacts')
            .upsert(contactPayloads, {
                onConflict:       'business_id, social_platform, social_id',
                ignoreDuplicates: false     // update phone/name on existing rows
            })
            .select('id, social_id');

        if (contactError) throw new Error(`Contacts upsert: ${contactError.message}`);

        contactMap   = insertedContacts.reduce((acc, c) => { acc[c.social_id] = c.id; return acc; }, {});
        stats.contacts = insertedContacts.length;
        console.log(`  [Contacts] ${stats.contacts} upserted`);
    }

    // ── 2. CONVERSATIONS ─────────────────────────────────────────
    const validChats = chats.filter(c => c.id && !isGroupOrBroadcast(c.id));

    const conversationPayloads = validChats
        .map(chat => ({
            business_id:  businessId,
            contact_id:   contactMap[chat.id] || null,
            external_id:  chat.id,
            channel:      'whatsapp',
            type:         'dm',
            status:       (chat.unreadCount || 0) > 0 ? 'open' : 'closed',
            unread_count: chat.unreadCount || 0
        }))
        .filter(c => c.contact_id !== null);

    let convoMap = {};

    if (conversationPayloads.length > 0) {
        const { data: insertedConvos, error: convoError } = await supabase
            .from('conversations')
            .upsert(conversationPayloads, { onConflict: 'business_id, contact_id' })
            .select('id, external_id');

        if (convoError) throw new Error(`Conversations upsert: ${convoError.message}`);

        convoMap           = insertedConvos.reduce((acc, c) => { acc[c.external_id] = c.id; return acc; }, {});
        stats.conversations = insertedConvos.length;
        console.log(`  [Conversations] ${stats.conversations} upserted`);
    }

    // ── 3. MESSAGES ──────────────────────────────────────────────
    const cutoffTs = Math.floor(Date.now() / 1000) - (HISTORY_DAYS * 24 * 60 * 60);

    // Track per-conversation metadata while iterating — avoids a second pass
    const convLastMsg     = {};   // convId → { ts, preview }
    const convLastInbound = {};   // convId → ts (unix)

    const messagePayloads = [];

    for (const msg of messages) {
        const jid = msg.key?.remoteJid;

        // ── Filters ──────────────────────────────────────────────
        if (!jid || isGroupOrBroadcast(jid))        { stats.skipped++; continue; }
        if ((msg.messageTimestamp || 0) < cutoffTs) { stats.skipped++; continue; }
        if (msg.messageStubType)                     { stats.skipped++; continue; } // system events

        const conversationId = convoMap[jid];
        const contactId      = contactMap[jid] || null;
        if (!conversationId)                         { stats.skipped++; continue; }

        const content = extractMessageContent(msg);
        if (!content)                                { stats.skipped++; continue; }

        const isFromMe = msg.key.fromMe === true;
        const ts       = msg.messageTimestamp;

        // ── Track conversation metadata ──────────────────────────
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
        }

        messagePayloads.push({
            whatsapp_message_id: msg.key.id,
            business_id:         businessId,
            contact_id:          contactId,          // bigint — no string conversion
            conversation_id:     conversationId,
            direction:           isFromMe ? 'out' : 'in',
            role:                isFromMe ? 'admin' : 'user',  // FIX: was 'ai' for fromMe
            agent_role:          isFromMe ? 'human' : 'legacy_ai',
            type:                content.type,
            content:             content,
            created_at:          new Date(ts * 1000).toISOString(),
            status:              'sent',
            is_read:             isFromMe,
            raw_payload:         msg
        });
    }

    // ── Batch upsert in chunks of 100 ────────────────────────────
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

    // ── 4. UPDATE CONVERSATION METADATA ─────────────────────────
    // Do this after messages so we have accurate last preview + timestamp
    const metaUpdates = Object.entries(convLastMsg).map(([convId, data]) => ({
        id:                   convId,
        last_message_preview: data.preview,
        last_user_message_at: convLastInbound[convId]
            ? new Date(convLastInbound[convId] * 1000).toISOString()
            : null
    }));

    let metaUpdated = 0;
    for (const { id, ...fields } of metaUpdates) {
        const { error } = await supabase
            .from('conversations')
            .update(fields)
            .eq('id', id);
        if (!error) metaUpdated++;
    }
    console.log(`  [Conversations] ${metaUpdated} metadata rows updated`);

    return stats;
}

// ==========================================
// 4. EXPRESS ROUTES
// ==========================================
app.post('/webhook/evolution', async (req, res) => {
    const businessId = req.query.business_id;
    const eventType  = req.body?.event;

    if (!businessId) {
        return res.status(400).json({ error: 'Missing business_id query param' });
    }

    // Respond immediately — Evolution API does not wait
    res.status(200).send('OK');

    if (eventType === 'messaging-history.set') {
        console.log(`\n[START] History sync — business: ${businessId}`);
        try {
            const stats = await processHistorySync(req.body, businessId);
            console.log(
                `[DONE] ${businessId} — ` +
                `contacts:${stats.contacts} | convos:${stats.conversations} | ` +
                `messages:${stats.messages} | skipped:${stats.skipped}`
            );
            await triggerPersonaPackGeneration(businessId);
        } catch (err) {
            console.error(`[ERROR] ${businessId}: ${err.message}`);
        }
    }
});

app.get('/health', (_req, res) => res.json({ status: 'ok' }));

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`WhatsApp sync service online — port ${PORT}`));
