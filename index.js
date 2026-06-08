-- ============================================================
-- WhatsApp Cleaner v4 — Migration & Frontend Contract
-- ============================================================
-- Run this against your Supabase project before deploying v4.
-- Project ref: xgtnbxdxbbywvzrttixf
-- ============================================================


-- ============================================================
-- 1. business_onboarding — new columns
-- ============================================================
-- These columns are the single source of truth the frontend polls.
-- All written by the cleaner service, read by the dashboard.

ALTER TABLE business_onboarding
    -- Lead activation state
    ADD COLUMN IF NOT EXISTS leads_total_found            INTEGER   DEFAULT 0,
    ADD COLUMN IF NOT EXISTS leads_auto_activated_count   INTEGER   DEFAULT 0,
    ADD COLUMN IF NOT EXISTS leads_auto_activated_at      TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS leads_user_activated_count   INTEGER   DEFAULT 0,

    -- Product discovery state
    ADD COLUMN IF NOT EXISTS products_auto_approved_count INTEGER   DEFAULT 0;

-- leads_processed already exists (used by follow-up engine as total activated)
-- products_pending_approval already exists
-- products_seeded already exists
-- sync_status already exists (JSONB)
-- current_step already exists


-- ============================================================
-- 2. product_image_staging — new columns
-- ============================================================
-- is_auto_approved: true if the cleaner approved it in first-run
-- (lets frontend distinguish auto vs user approved for audit trail)

ALTER TABLE product_image_staging
    ADD COLUMN IF NOT EXISTS is_auto_approved BOOLEAN  DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS approved_at      TIMESTAMPTZ;

-- Ensure upsert conflict target exists
-- (cleaner uses onConflict: 'business_id, media_url')
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'product_image_staging_business_id_media_url_key'
    ) THEN
        ALTER TABLE product_image_staging
            ADD CONSTRAINT product_image_staging_business_id_media_url_key
            UNIQUE (business_id, media_url);
    END IF;
END $$;


-- ============================================================
-- 3. Useful index for product staging queries
-- ============================================================
CREATE INDEX IF NOT EXISTS idx_product_image_staging_business_status
    ON product_image_staging (business_id, status);


-- ============================================================
-- FRONTEND POLLING CONTRACT
-- ============================================================
-- The dashboard polls: SELECT * FROM business_onboarding WHERE business_id = $1
-- It reads sync_status.stage to drive the onboarding UI.
--
-- Stage flow and what the frontend should render:
--
-- 'history_received'
--     → Show: "Syncing your WhatsApp..." spinner
--     → No slider yet
--
-- 'awaiting_lead_activation'
--     → Show: Slider, pre-filled to MIN(30, leads_total_found)
--     → Label: "X contacts found. Choose how many to activate."
--     → Field to read: leads_total_found  (slider max)
--     → Note: Bot build triggers automatically at 30, slider is for top-up
--
-- 'auto_activating'
--     → Show: Inline notice "Setting up your first 30 leads..."
--     → Slider disabled briefly
--
-- 'insufficient_funds_auto'
--     → Show: "Top up your balance to activate leads"
--     → CTA: "Top up" button
--     → Fields: sync_status.required_kes
--
-- 'leads_activated'
--     → Show: Confirmation row "X leads activated"
--     → Fields: sync_status.total_activated, sync_status.auto_activated,
--               sync_status.user_activated, sync_status.last_charged_kes
--     → Slider still visible for adding more (calls /leads/activate)
--
-- 'bot_building'
--     → Show: Bot build progress card "Building your AI bot..."
--     → Poll: businesses.persona_pack_status until 'ready'
--
-- 'bot_build_failed'
--     → Show: Error card with retry CTA
--
-- 'products_auto_approved'
--     → Show: "X product images sent for analysis"
--     → Fields: sync_status.auto_approved, sync_status.pending
--     → If pending > 0: show "Approve X more" CTA → POST /products/approve-discovery
--
-- 'products_discovered'
--     → Show: Product approval CTA
--     → Fields: sync_status.pending
--     → CTA: "Approve X product images" → POST /products/approve-discovery
--
-- 'products_approved'
--     → Show: "X products being analysed" confirmation
--
-- 'connected'
--     → Show: WhatsApp connected banner, advance step indicator
--
-- 'disconnected'
--     → Show: Reconnect CTA
--
--
-- /leads/activate endpoint contract:
--   POST { business_id, count }   ← count is TOTAL desired (e.g. 1500)
--   200 { ok, delta_activated, total_activated, charged_kes, charged_usd }
--   400 { error: 'already_activated', already_activated }
--   402 { error: 'insufficient_funds', required_kes, delta, reason }
--
-- Slider math on the frontend:
--   already_activated = leads_auto_activated_count + leads_user_activated_count
--   delta_cost_kes    = (slider_value - already_activated) * price_per_lead
--   → Show cost preview before user confirms
--   → On confirm: POST /leads/activate with { business_id, count: slider_value }
--
--
-- /products/approve-discovery endpoint contract:
--   POST { business_id }
--   200 { ok, approved_count, total_approved }
