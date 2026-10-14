-- =============================================================================
-- SportZone OLTP Schema  (Neon Postgres)
-- Normalised 3NF relational schema — source of truth for transactions
-- =============================================================================

-- ── Lookup / reference tables ────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS oltp.category (
    category_id     SERIAL PRIMARY KEY,
    name            VARCHAR(100) NOT NULL UNIQUE,
    parent_id       INTEGER REFERENCES oltp.category(category_id)
);

CREATE TABLE IF NOT EXISTS oltp.supplier (
    supplier_id     SERIAL PRIMARY KEY,
    name            VARCHAR(200) NOT NULL,
    contact_email   VARCHAR(255),
    phone           VARCHAR(50),
    country         CHAR(2),
    is_active       BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS oltp.brand (
    brand_id        SERIAL PRIMARY KEY,
    name            VARCHAR(100) NOT NULL UNIQUE,
    country_origin  CHAR(2),
    is_active       BOOLEAN DEFAULT TRUE
);

-- ── Product catalogue ─────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS oltp.product (
    product_id      SERIAL PRIMARY KEY,
    sku             VARCHAR(50)  NOT NULL UNIQUE,
    name            VARCHAR(255) NOT NULL,
    category_id     INTEGER NOT NULL REFERENCES oltp.category(category_id),
    brand_id        INTEGER NOT NULL REFERENCES oltp.brand(brand_id),
    supplier_id     INTEGER NOT NULL REFERENCES oltp.supplier(supplier_id),
    unit_cost       NUMERIC(10,2) NOT NULL,
    unit_price      NUMERIC(10,2) NOT NULL,
    weight_kg       NUMERIC(6,3),
    is_active       BOOLEAN DEFAULT TRUE,
    launch_date     DATE DEFAULT CURRENT_DATE,
    discontinue_dt  DATE,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS oltp.product_price_history (
    price_history_id SERIAL PRIMARY KEY,
    product_id       INTEGER NOT NULL REFERENCES oltp.product(product_id),
    old_price        NUMERIC(10,2),
    new_price        NUMERIC(10,2),
    changed_at       TIMESTAMPTZ DEFAULT NOW(),
    changed_by       VARCHAR(100)
);

-- ── Inventory ─────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS oltp.store (
    store_id        SERIAL PRIMARY KEY,
    code            VARCHAR(20)  NOT NULL UNIQUE,
    name            VARCHAR(200) NOT NULL,
    store_type      VARCHAR(20)  NOT NULL CHECK (store_type IN ('PHYSICAL','ONLINE')),
    region          VARCHAR(50),
    address         TEXT,
    city            VARCHAR(100),
    state           VARCHAR(50),
    zip_code        VARCHAR(20),
    country         CHAR(2),
    open_date       DATE,
    manager_name    VARCHAR(200),
    is_active       BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS oltp.inventory (
    inventory_id    SERIAL PRIMARY KEY,
    product_id      INTEGER NOT NULL REFERENCES oltp.product(product_id),
    store_id        INTEGER NOT NULL REFERENCES oltp.store(store_id),
    qty_on_hand     INTEGER NOT NULL DEFAULT 0,
    qty_reserved    INTEGER NOT NULL DEFAULT 0,
    reorder_point   INTEGER NOT NULL DEFAULT 10,
    last_updated    TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (product_id, store_id)
);

-- ── Customer ──────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS oltp.customer (
    customer_id     SERIAL PRIMARY KEY,
    external_id     VARCHAR(30)  UNIQUE,          -- CRM / external reference
    first_name      VARCHAR(100) NOT NULL,
    last_name       VARCHAR(100) NOT NULL,
    email           VARCHAR(255) NOT NULL UNIQUE,
    phone           VARCHAR(50),
    date_of_birth   DATE,
    gender          CHAR(1) CHECK (gender IN ('M','F','U')),
    address_line1   VARCHAR(200),
    address_line2   VARCHAR(200),
    city            VARCHAR(100),
    state           VARCHAR(50),
    zip_code        VARCHAR(20),
    country         CHAR(2),
    loyalty_tier    VARCHAR(20)  DEFAULT 'BRONZE' CHECK (loyalty_tier IN ('BRONZE','SILVER','GOLD','PLATINUM')),
    loyalty_points  INTEGER DEFAULT 0,
    signup_date     DATE DEFAULT CURRENT_DATE,
    is_active       BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

-- ── Promotions ────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS oltp.promotion (
    promotion_id    SERIAL PRIMARY KEY,
    code            VARCHAR(50)  NOT NULL UNIQUE,
    name            VARCHAR(200) NOT NULL,
    promo_type      VARCHAR(30)  NOT NULL CHECK (promo_type IN ('PERCENTAGE','FIXED','BOGO','FREESHIP')),
    discount_value  NUMERIC(10,2) NOT NULL DEFAULT 0,
    min_order_amt   NUMERIC(10,2) DEFAULT 0,
    max_uses        INTEGER,
    uses_count      INTEGER DEFAULT 0,
    start_date      DATE NOT NULL,
    end_date        DATE NOT NULL,
    is_active       BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

-- ── Orders ────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS oltp.order_header (
    order_id        SERIAL PRIMARY KEY,
    order_ref       VARCHAR(30)  NOT NULL UNIQUE,  -- human-readable e.g. ORD-2024-001234
    customer_id     INTEGER NOT NULL REFERENCES oltp.customer(customer_id),
    store_id        INTEGER NOT NULL REFERENCES oltp.store(store_id),
    promotion_id    INTEGER REFERENCES oltp.promotion(promotion_id),
    channel         VARCHAR(20)  NOT NULL CHECK (channel IN ('ONLINE','IN-STORE','MOBILE','PHONE')),
    order_status    VARCHAR(20)  NOT NULL DEFAULT 'PENDING'
                    CHECK (order_status IN ('PENDING','CONFIRMED','SHIPPED','COMPLETED','CANCELLED','RETURNED')),
    order_date      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    shipped_date    TIMESTAMPTZ,
    delivered_date  TIMESTAMPTZ,
    subtotal_amt    NUMERIC(12,2) DEFAULT 0,
    discount_amt    NUMERIC(12,2) DEFAULT 0,
    tax_amt         NUMERIC(12,2) DEFAULT 0,
    shipping_amt    NUMERIC(12,2) DEFAULT 0,
    total_amt       NUMERIC(12,2) DEFAULT 0,
    currency        CHAR(3) DEFAULT 'USD',
    notes           TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS oltp.order_item (
    order_item_id   SERIAL PRIMARY KEY,
    order_id        INTEGER NOT NULL REFERENCES oltp.order_header(order_id),
    product_id      INTEGER NOT NULL REFERENCES oltp.product(product_id),
    qty             INTEGER NOT NULL CHECK (qty > 0),
    unit_price      NUMERIC(10,2) NOT NULL,
    unit_cost       NUMERIC(10,2) NOT NULL,
    discount_pct    NUMERIC(5,2)  DEFAULT 0,
    line_subtotal   NUMERIC(12,2) NOT NULL,   -- qty * unit_price
    line_discount   NUMERIC(12,2) DEFAULT 0,  -- discount amount
    line_total      NUMERIC(12,2) NOT NULL,   -- after discount
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

-- ── Payments ──────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS oltp.payment (
    payment_id      SERIAL PRIMARY KEY,
    order_id        INTEGER NOT NULL REFERENCES oltp.order_header(order_id),
    payment_method  VARCHAR(30) NOT NULL CHECK (payment_method IN ('CREDIT_CARD','DEBIT_CARD','PAYPAL','APPLE_PAY','GOOGLE_PAY','GIFT_CARD','CASH')),
    payment_status  VARCHAR(20) NOT NULL DEFAULT 'PENDING' CHECK (payment_status IN ('PENDING','AUTHORIZED','CAPTURED','FAILED','REFUNDED')),
    amount          NUMERIC(12,2) NOT NULL,
    currency        CHAR(3) DEFAULT 'USD',
    transaction_ref VARCHAR(100),
    processed_at    TIMESTAMPTZ DEFAULT NOW()
);

-- ── Customer events (clickstream simplified) ─────────────────────────────────

CREATE TABLE IF NOT EXISTS oltp.customer_event (
    event_id        BIGSERIAL PRIMARY KEY,
    customer_id     INTEGER REFERENCES oltp.customer(customer_id),
    session_id      VARCHAR(50),
    event_type      VARCHAR(50) NOT NULL CHECK (event_type IN ('PAGE_VIEW','ADD_TO_CART','REMOVE_FROM_CART','CHECKOUT_START','PURCHASE','RETURN_INIT','REVIEW')),
    product_id      INTEGER REFERENCES oltp.product(product_id),
    event_data      JSONB,
    occurred_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ── ETL watermark tracking ────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS oltp.etl_watermark (
    table_name      VARCHAR(100) PRIMARY KEY,
    last_extracted  TIMESTAMPTZ DEFAULT '1970-01-01',
    last_run_at     TIMESTAMPTZ DEFAULT NOW(),
    rows_extracted  INTEGER DEFAULT 0
);

-- ── Indexes ───────────────────────────────────────────────────────────────────

CREATE INDEX IF NOT EXISTS idx_order_customer   ON oltp.order_header(customer_id);
CREATE INDEX IF NOT EXISTS idx_order_store      ON oltp.order_header(store_id);
CREATE INDEX IF NOT EXISTS idx_order_date       ON oltp.order_header(order_date);
CREATE INDEX IF NOT EXISTS idx_order_status     ON oltp.order_header(order_status);
CREATE INDEX IF NOT EXISTS idx_order_item_order ON oltp.order_item(order_id);
CREATE INDEX IF NOT EXISTS idx_order_item_prod  ON oltp.order_item(product_id);
CREATE INDEX IF NOT EXISTS idx_inventory_prod   ON oltp.inventory(product_id);
CREATE INDEX IF NOT EXISTS idx_cust_email       ON oltp.customer(email);
CREATE INDEX IF NOT EXISTS idx_cust_loyalty     ON oltp.customer(loyalty_tier);
CREATE INDEX IF NOT EXISTS idx_event_cust       ON oltp.customer_event(customer_id);
CREATE INDEX IF NOT EXISTS idx_event_ts         ON oltp.customer_event(occurred_at);
CREATE INDEX IF NOT EXISTS idx_payment_order    ON oltp.payment(order_id);
CREATE INDEX IF NOT EXISTS idx_product_category ON oltp.product(category_id);
CREATE INDEX IF NOT EXISTS idx_product_sku      ON oltp.product(sku);
