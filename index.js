import express from 'express';
import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
import WebSocket from 'ws'; // Use capitalized 'WebSocket' to avoid naming conflicts

dotenv.config();

const app = express();
app.use(express.json({ limit: '50mb' }));

// Initialize Supabase with the WebSocket transport for Node 20 compatibility
const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_SERVICE_KEY,
    {
        auth: {
            persistSession: false
        },
        realtime: {
            websocket: WebSocket // This is the key fix
        }
    }
);

app.post('/webhook/evolution', async (req, res) => {
    const businessId = req.query.business_id;
    const eventType = req.body.event;

    if (!businessId) {
        return res.status(400).send("Missing business_id in webhook URL");
    }

    // Always respond 200 immediately to the sender
    res.status(200).send("OK");

    if (eventType === 'messaging-history.set') {
        try {
            console.log(`[Processing] Starting history sync for: ${businessId}`);
            await processHistorySync(req.body, businessId);
            console.log(`[Success] History synced for: ${businessId}`);
        } catch (error) {
            console.error(`[Error] Sync failed:`, error.message);
        }
    }
});

async function processHistorySync(payload, businessId) {
    const { contacts, chats, messages } = payload.data;
    
    // 1. Upsert Contacts
    const contactPayloads = contacts.map(c => ({
        business_id: businessId,
        social_platform: 'whatsapp',
        social_id: c.id,
        name: c.name || c.notify || 'Unknown',
        channel: 'whatsapp'
    }));

    const { data: insertedContacts, error: contactError } = await supabase
        .from('contacts')
        .upsert(contactPayloads, { onConflict: 'business_id, social_platform, social_id' })
        .select('id, social_id');

    if (contactError) throw contactError;

    const contactMap = insertedContacts.reduce((acc, curr) => {
        acc[curr.social_id] = curr.id;
        return acc;
    }, {});

    // 2. Upsert Conversations
    const conversationPayloads = chats.map(chat => ({
        business_id: businessId,
        contact_id: contactMap[chat.id],
        external_id: chat.id,
        channel: 'whatsapp',
        type: 'dm',
        status: chat.unreadCount > 0 ? 'open' : 'closed',
        unread_count: chat.unreadCount || 0
    })).filter(c => c.contact_id);

    const { data: insertedConvos, error: convoError } = await supabase
        .from('conversations')
        .upsert(conversationPayloads, { onConflict: 'business_id, contact_id' })
        .select('id, external_id');

    if (convoError) throw convoError;

    const convoMap = insertedConvos.reduce((acc, curr) => {
        acc[curr.external_id] = curr.id;
        return acc;
    }, {});

    // 3. Upsert Messages (3 Month Filter)
    const threeMonthsAgo = Math.floor(Date.now() / 1000) - (90 * 24 * 60 * 60);

    const messagePayloads = messages
        .filter(msg => msg.messageTimestamp > threeMonthsAgo)
        .map(msg => {
            const jid = msg.key.remoteJid;
            const textContent = msg.message?.conversation || msg.message?.extendedTextMessage?.text || "";
            
            return {
                whatsapp_message_id: msg.key.id,
                business_id: businessId,
                contact_id: contactMap[jid],
                conversation_id: convoMap[jid],
                direction: msg.key.fromMe ? 'out' : 'in',
                role: msg.key.fromMe ? 'ai' : 'user',
                content: { text: textContent },
                created_at: new Date(msg.messageTimestamp * 1000).toISOString(),
                status: 'sent',
                is_read: msg.key.fromMe ? true : false,
                raw_payload: msg
            };
        })
        .filter(m => m.content.text !== "" && m.conversation_id);

    // Batch upload to prevent timeouts
    for (let i = 0; i < messagePayloads.length; i += 100) {
        const chunk = messagePayloads.slice(i, i + 100);
        const { error: msgError } = await supabase
            .from('messages')
            .upsert(chunk, { onConflict: 'whatsapp_message_id' });
        
        if (msgError) console.error("Chunk error:", msgError.message);
    }
}

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Cleaner server online on port ${PORT}`));
