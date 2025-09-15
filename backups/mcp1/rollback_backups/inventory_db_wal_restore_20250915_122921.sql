--
-- PostgreSQL database dump
--

\restrict YkMPkQb73a3oSAnRujpB4yn5kjhsBHCiYXbhaxSVtz9HedqiInSTzJrxDXHzDRL

-- Dumped from database version 17.6 (Postgres.app)
-- Dumped by pg_dump version 17.6 (Postgres.app)

-- Started on 2025-09-15 12:29:21 IST

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- TOC entry 220 (class 1259 OID 34195)
-- Name: inventory; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.inventory (
    inventory_id integer NOT NULL,
    product_id integer,
    warehouse_location character varying(50) NOT NULL,
    quantity_on_hand integer DEFAULT 0 NOT NULL,
    reorder_level integer DEFAULT 10 NOT NULL,
    last_updated timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT inventory_quantity_on_hand_check CHECK ((quantity_on_hand >= 0)),
    CONSTRAINT inventory_reorder_level_check CHECK ((reorder_level >= 0))
);


ALTER TABLE public.inventory OWNER TO postgres;

--
-- TOC entry 219 (class 1259 OID 34194)
-- Name: inventory_inventory_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.inventory_inventory_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.inventory_inventory_id_seq OWNER TO postgres;

--
-- TOC entry 3712 (class 0 OID 0)
-- Dependencies: 219
-- Name: inventory_inventory_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.inventory_inventory_id_seq OWNED BY public.inventory.inventory_id;


--
-- TOC entry 222 (class 1259 OID 34212)
-- Name: inventory_movements; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.inventory_movements (
    movement_id integer NOT NULL,
    product_id integer,
    movement_type character varying(20) NOT NULL,
    quantity_change integer NOT NULL,
    movement_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    reference_number character varying(50),
    notes text,
    CONSTRAINT valid_movement_type CHECK (((movement_type)::text = ANY ((ARRAY['inbound'::character varying, 'outbound'::character varying, 'adjustment'::character varying])::text[])))
);


ALTER TABLE public.inventory_movements OWNER TO postgres;

--
-- TOC entry 221 (class 1259 OID 34211)
-- Name: inventory_movements_movement_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.inventory_movements_movement_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.inventory_movements_movement_id_seq OWNER TO postgres;

--
-- TOC entry 3713 (class 0 OID 0)
-- Dependencies: 221
-- Name: inventory_movements_movement_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.inventory_movements_movement_id_seq OWNED BY public.inventory_movements.movement_id;


--
-- TOC entry 218 (class 1259 OID 34182)
-- Name: products; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.products (
    product_id integer NOT NULL,
    sku character varying(50) NOT NULL,
    product_name character varying(100) NOT NULL,
    description text,
    category character varying(50),
    unit_price numeric(8,2) NOT NULL,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT products_unit_price_check CHECK ((unit_price >= (0)::numeric))
);


ALTER TABLE public.products OWNER TO postgres;

--
-- TOC entry 217 (class 1259 OID 34181)
-- Name: products_product_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.products_product_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.products_product_id_seq OWNER TO postgres;

--
-- TOC entry 3714 (class 0 OID 0)
-- Dependencies: 217
-- Name: products_product_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.products_product_id_seq OWNED BY public.products.product_id;


--
-- TOC entry 3530 (class 2604 OID 34198)
-- Name: inventory inventory_id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.inventory ALTER COLUMN inventory_id SET DEFAULT nextval('public.inventory_inventory_id_seq'::regclass);


--
-- TOC entry 3534 (class 2604 OID 34215)
-- Name: inventory_movements movement_id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.inventory_movements ALTER COLUMN movement_id SET DEFAULT nextval('public.inventory_movements_movement_id_seq'::regclass);


--
-- TOC entry 3528 (class 2604 OID 34185)
-- Name: products product_id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.products ALTER COLUMN product_id SET DEFAULT nextval('public.products_product_id_seq'::regclass);


--
-- TOC entry 3704 (class 0 OID 34195)
-- Dependencies: 220
-- Data for Name: inventory; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.inventory (inventory_id, product_id, warehouse_location, quantity_on_hand, reorder_level, last_updated) FROM stdin;
2	2	Warehouse-A	120	25	2025-09-15 12:07:12.952927
3	3	Warehouse-B	8	5	2025-09-15 12:07:12.952927
4	4	Warehouse-A	32	15	2025-09-15 12:07:12.952927
5	5	Warehouse-A	67	20	2025-09-15 12:07:12.952927
6	6	Warehouse-B	15	8	2025-09-15 12:07:12.952927
7	7	Warehouse-A	88	30	2025-09-15 12:07:12.952927
8	8	Warehouse-A	42	12	2025-09-15 12:07:12.952927
1	1	Warehouse-Aaabbcc	45	10	2025-09-15 12:07:12.952927
\.


--
-- TOC entry 3706 (class 0 OID 34212)
-- Dependencies: 222
-- Data for Name: inventory_movements; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.inventory_movements (movement_id, product_id, movement_type, quantity_change, movement_date, reference_number, notes) FROM stdin;
1	1	inbound	50	2025-09-15 12:07:12.953413	PO-2025-001	Received shipment from supplier
2	2	outbound	-25	2025-09-15 12:07:12.953413	SO-2025-015	Sold to customer order
3	3	adjustment	-2	2025-09-15 12:07:12.953413	ADJ-2025-003	Damaged units removed
4	4	inbound	40	2025-09-15 12:07:12.953413	PO-2025-002	New stock arrival
5	5	outbound	-15	2025-09-15 12:07:12.953413	SO-2025-016	Bulk order shipment
6	6	inbound	20	2025-09-15 12:07:12.953413	PO-2025-003	Restocking furniture
7	7	outbound	-12	2025-09-15 12:07:12.953413	SO-2025-017	Corporate order
8	8	adjustment	5	2025-09-15 12:07:12.953413	ADJ-2025-004	Inventory count correction
\.


--
-- TOC entry 3702 (class 0 OID 34182)
-- Dependencies: 218
-- Data for Name: products; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.products (product_id, sku, product_name, description, category, unit_price, created_at) FROM stdin;
1	LAP-001	Business Laptop	15-inch business laptop with SSD	Electronics	899.99	2025-09-15 12:07:12.952285
2	MOU-001	Wireless Mouse	Ergonomic wireless mouse	Electronics	29.99	2025-09-15 12:07:12.952285
3	DES-001	Standing Desk	Adjustable height standing desk	Furniture	349.99	2025-09-15 12:07:12.952285
4	MON-001	Monitor 24inch	24-inch LED monitor	Electronics	199.99	2025-09-15 12:07:12.952285
5	KEY-001	Mechanical Keyboard	RGB mechanical keyboard	Electronics	129.99	2025-09-15 12:07:12.952285
6	CHR-001	Office Chair	Ergonomic office chair	Furniture	299.99	2025-09-15 12:07:12.952285
7	CAM-001	Webcam HD	1080p HD webcam	Electronics	79.99	2025-09-15 12:07:12.952285
8	HED-001	Noise Cancelling Headphones	Wireless noise cancelling headphones	Electronics	199.99	2025-09-15 12:07:12.952285
\.


--
-- TOC entry 3715 (class 0 OID 0)
-- Dependencies: 219
-- Name: inventory_inventory_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.inventory_inventory_id_seq', 8, true);


--
-- TOC entry 3716 (class 0 OID 0)
-- Dependencies: 221
-- Name: inventory_movements_movement_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.inventory_movements_movement_id_seq', 8, true);


--
-- TOC entry 3717 (class 0 OID 0)
-- Dependencies: 217
-- Name: products_product_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.products_product_id_seq', 8, true);


--
-- TOC entry 3553 (class 2606 OID 34221)
-- Name: inventory_movements inventory_movements_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.inventory_movements
    ADD CONSTRAINT inventory_movements_pkey PRIMARY KEY (movement_id);


--
-- TOC entry 3549 (class 2606 OID 34205)
-- Name: inventory inventory_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.inventory
    ADD CONSTRAINT inventory_pkey PRIMARY KEY (inventory_id);


--
-- TOC entry 3543 (class 2606 OID 34191)
-- Name: products products_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.products
    ADD CONSTRAINT products_pkey PRIMARY KEY (product_id);


--
-- TOC entry 3545 (class 2606 OID 34193)
-- Name: products products_sku_key; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.products
    ADD CONSTRAINT products_sku_key UNIQUE (sku);


--
-- TOC entry 3546 (class 1259 OID 34229)
-- Name: idx_inventory_product_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_inventory_product_id ON public.inventory USING btree (product_id);


--
-- TOC entry 3547 (class 1259 OID 34230)
-- Name: idx_inventory_warehouse; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_inventory_warehouse ON public.inventory USING btree (warehouse_location);


--
-- TOC entry 3550 (class 1259 OID 34232)
-- Name: idx_movements_date; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_movements_date ON public.inventory_movements USING btree (movement_date);


--
-- TOC entry 3551 (class 1259 OID 34231)
-- Name: idx_movements_product_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_movements_product_id ON public.inventory_movements USING btree (product_id);


--
-- TOC entry 3540 (class 1259 OID 34228)
-- Name: idx_products_category; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_products_category ON public.products USING btree (category);


--
-- TOC entry 3541 (class 1259 OID 34227)
-- Name: idx_products_sku; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_products_sku ON public.products USING btree (sku);


--
-- TOC entry 3555 (class 2606 OID 34222)
-- Name: inventory_movements inventory_movements_product_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.inventory_movements
    ADD CONSTRAINT inventory_movements_product_id_fkey FOREIGN KEY (product_id) REFERENCES public.products(product_id);


--
-- TOC entry 3554 (class 2606 OID 34206)
-- Name: inventory inventory_product_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.inventory
    ADD CONSTRAINT inventory_product_id_fkey FOREIGN KEY (product_id) REFERENCES public.products(product_id);


-- Completed on 2025-09-15 12:29:21 IST

--
-- PostgreSQL database dump complete
--

\unrestrict YkMPkQb73a3oSAnRujpB4yn5kjhsBHCiYXbhaxSVtz9HedqiInSTzJrxDXHzDRL

