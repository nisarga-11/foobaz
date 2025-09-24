--
-- PostgreSQL database dump
--

\restrict FcqGudY7sztGoEyH3JpMDqg7fxZnz9n9PiHVuGNF6GQfhSmSDdr2f9ADScVvggN

-- Dumped from database version 17.6 (Postgres.app)
-- Dumped by pg_dump version 17.6 (Postgres.app)

-- Started on 2025-09-22 12:58:50 IST

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
-- TOC entry 220 (class 1259 OID 33752)
-- Name: customer_analytics; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.customer_analytics (
    analytics_id integer NOT NULL,
    customer_id integer NOT NULL,
    total_lifetime_value numeric(10,2) NOT NULL,
    order_frequency numeric(4,2),
    last_order_date date,
    customer_segment character varying(30),
    calculated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT customer_analytics_order_frequency_check CHECK ((order_frequency >= (0)::numeric)),
    CONSTRAINT customer_analytics_total_lifetime_value_check CHECK ((total_lifetime_value >= (0)::numeric)),
    CONSTRAINT valid_segment CHECK (((customer_segment)::text = ANY ((ARRAY['high_value'::character varying, 'regular'::character varying, 'at_risk'::character varying, 'new'::character varying])::text[])))
);


ALTER TABLE public.customer_analytics OWNER TO postgres;

--
-- TOC entry 219 (class 1259 OID 33751)
-- Name: customer_analytics_analytics_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.customer_analytics_analytics_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.customer_analytics_analytics_id_seq OWNER TO postgres;

--
-- TOC entry 3709 (class 0 OID 0)
-- Dependencies: 219
-- Name: customer_analytics_analytics_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.customer_analytics_analytics_id_seq OWNED BY public.customer_analytics.analytics_id;


--
-- TOC entry 222 (class 1259 OID 33763)
-- Name: product_analytics; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.product_analytics (
    product_analytics_id integer NOT NULL,
    product_sku character varying(50) NOT NULL,
    total_units_sold integer DEFAULT 0,
    total_revenue numeric(12,2) DEFAULT 0.00,
    avg_selling_price numeric(8,2),
    last_sale_date date,
    trend character varying(20),
    analysis_date date DEFAULT CURRENT_DATE NOT NULL
);


ALTER TABLE public.product_analytics OWNER TO postgres;

--
-- TOC entry 221 (class 1259 OID 33762)
-- Name: product_analytics_product_analytics_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.product_analytics_product_analytics_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.product_analytics_product_analytics_id_seq OWNER TO postgres;

--
-- TOC entry 3710 (class 0 OID 0)
-- Dependencies: 221
-- Name: product_analytics_product_analytics_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.product_analytics_product_analytics_id_seq OWNED BY public.product_analytics.product_analytics_id;


--
-- TOC entry 218 (class 1259 OID 33741)
-- Name: sales_metrics; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.sales_metrics (
    metric_id integer NOT NULL,
    date_recorded date NOT NULL,
    total_sales numeric(12,2) NOT NULL,
    order_count integer NOT NULL,
    avg_order_value numeric(8,2) NOT NULL,
    top_selling_category character varying(50),
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT sales_metrics_avg_order_value_check CHECK ((avg_order_value >= (0)::numeric)),
    CONSTRAINT sales_metrics_order_count_check CHECK ((order_count >= 0)),
    CONSTRAINT sales_metrics_total_sales_check CHECK ((total_sales >= (0)::numeric))
);


ALTER TABLE public.sales_metrics OWNER TO postgres;

--
-- TOC entry 217 (class 1259 OID 33740)
-- Name: sales_metrics_metric_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.sales_metrics_metric_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.sales_metrics_metric_id_seq OWNER TO postgres;

--
-- TOC entry 3711 (class 0 OID 0)
-- Dependencies: 217
-- Name: sales_metrics_metric_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.sales_metrics_metric_id_seq OWNED BY public.sales_metrics.metric_id;


--
-- TOC entry 3530 (class 2604 OID 33755)
-- Name: customer_analytics analytics_id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.customer_analytics ALTER COLUMN analytics_id SET DEFAULT nextval('public.customer_analytics_analytics_id_seq'::regclass);


--
-- TOC entry 3532 (class 2604 OID 33766)
-- Name: product_analytics product_analytics_id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.product_analytics ALTER COLUMN product_analytics_id SET DEFAULT nextval('public.product_analytics_product_analytics_id_seq'::regclass);


--
-- TOC entry 3528 (class 2604 OID 33744)
-- Name: sales_metrics metric_id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.sales_metrics ALTER COLUMN metric_id SET DEFAULT nextval('public.sales_metrics_metric_id_seq'::regclass);


--
-- TOC entry 3701 (class 0 OID 33752)
-- Dependencies: 220
-- Data for Name: customer_analytics; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.customer_analytics (analytics_id, customer_id, total_lifetime_value, order_frequency, last_order_date, customer_segment, calculated_at) FROM stdin;
4	4	79.99	0.50	2025-09-05	new	2025-09-13 10:50:00.660948
6	6	129.99	0.80	2025-09-06	new	2025-09-13 10:50:00.660948
7	7	0.00	0.00	\N	new	2025-09-13 10:50:00.660948
8	8	0.00	0.00	\N	new	2025-09-13 10:50:00.660948
5	5	349.99	1.00	2025-09-09	new	2025-09-13 10:50:00.660948
1	1	389.98	2.50	2025-09-10	regular	2025-09-13 10:50:00.660948
2	2	349.49	1.80	2025-09-08	regular	2025-09-13 10:50:00.660948
3	3	549.99	1.20	2025-09-07	high_value	2025-09-13 10:50:00.660948
9	1	389.98	2.50	2025-09-10	regular	2025-09-13 21:14:52.926987
10	2	349.49	1.80	2025-09-08	regular	2025-09-13 21:14:52.926987
11	3	549.99	1.20	2025-09-07	high_value	2025-09-13 21:14:52.926987
12	4	79.99	0.50	2025-09-05	new	2025-09-13 21:14:52.926987
13	5	349.99	1.00	2025-09-09	regular	2025-09-13 21:14:52.926987
14	6	129.99	0.80	2025-09-06	new	2025-09-13 21:14:52.926987
15	7	0.00	0.00	\N	new	2025-09-13 21:14:52.926987
16	8	0.00	0.00	\N	new	2025-09-13 21:14:52.926987
17	1	389.98	2.50	2025-09-10	regular	2025-09-13 22:06:21.616962
18	2	349.49	1.80	2025-09-08	regular	2025-09-13 22:06:21.616962
19	3	549.99	1.20	2025-09-07	high_value	2025-09-13 22:06:21.616962
20	4	79.99	0.50	2025-09-05	new	2025-09-13 22:06:21.616962
21	5	349.99	1.00	2025-09-09	regular	2025-09-13 22:06:21.616962
22	6	129.99	0.80	2025-09-06	new	2025-09-13 22:06:21.616962
23	7	0.00	0.00	\N	new	2025-09-13 22:06:21.616962
24	8	0.00	0.00	\N	new	2025-09-13 22:06:21.616962
25	1	389.98	2.50	2025-09-10	regular	2025-09-13 22:33:46.16128
26	2	349.49	1.80	2025-09-08	regular	2025-09-13 22:33:46.16128
27	3	549.99	1.20	2025-09-07	high_value	2025-09-13 22:33:46.16128
28	4	79.99	0.50	2025-09-05	new	2025-09-13 22:33:46.16128
29	5	349.99	1.00	2025-09-09	regular	2025-09-13 22:33:46.16128
30	6	129.99	0.80	2025-09-06	new	2025-09-13 22:33:46.16128
31	7	0.00	0.00	\N	new	2025-09-13 22:33:46.16128
32	8	0.00	0.00	\N	new	2025-09-13 22:33:46.16128
33	1	389.98	2.50	2025-09-10	regular	2025-09-15 12:07:12.9923
34	2	349.49	1.80	2025-09-08	regular	2025-09-15 12:07:12.9923
35	3	549.99	1.20	2025-09-07	high_value	2025-09-15 12:07:12.9923
36	4	79.99	0.50	2025-09-05	new	2025-09-15 12:07:12.9923
37	5	349.99	1.00	2025-09-09	regular	2025-09-15 12:07:12.9923
38	6	129.99	0.80	2025-09-06	new	2025-09-15 12:07:12.9923
39	7	0.00	0.00	\N	new	2025-09-15 12:07:12.9923
40	8	0.00	0.00	\N	new	2025-09-15 12:07:12.9923
41	1	389.98	2.50	2025-09-10	regular	2025-09-15 12:29:22.025565
42	2	349.49	1.80	2025-09-08	regular	2025-09-15 12:29:22.025565
43	3	549.99	1.20	2025-09-07	high_value	2025-09-15 12:29:22.025565
44	4	79.99	0.50	2025-09-05	new	2025-09-15 12:29:22.025565
45	5	349.99	1.00	2025-09-09	regular	2025-09-15 12:29:22.025565
46	6	129.99	0.80	2025-09-06	new	2025-09-15 12:29:22.025565
47	7	0.00	0.00	\N	new	2025-09-15 12:29:22.025565
48	8	0.00	0.00	\N	new	2025-09-15 12:29:22.025565
49	1	389.98	2.50	2025-09-10	regular	2025-09-15 15:17:19.2753
50	2	349.49	1.80	2025-09-08	regular	2025-09-15 15:17:19.2753
51	3	549.99	1.20	2025-09-07	high_value	2025-09-15 15:17:19.2753
52	4	79.99	0.50	2025-09-05	new	2025-09-15 15:17:19.2753
53	5	349.99	1.00	2025-09-09	regular	2025-09-15 15:17:19.2753
54	6	129.99	0.80	2025-09-06	new	2025-09-15 15:17:19.2753
55	7	0.00	0.00	\N	new	2025-09-15 15:17:19.2753
56	8	0.00	0.00	\N	new	2025-09-15 15:17:19.2753
57	1	389.98	2.50	2025-09-10	regular	2025-09-17 18:55:47.959985
58	2	349.49	1.80	2025-09-08	regular	2025-09-17 18:55:47.959985
59	3	549.99	1.20	2025-09-07	high_value	2025-09-17 18:55:47.959985
60	4	79.99	0.50	2025-09-05	new	2025-09-17 18:55:47.959985
61	5	349.99	1.00	2025-09-09	regular	2025-09-17 18:55:47.959985
62	6	129.99	0.80	2025-09-06	new	2025-09-17 18:55:47.959985
63	7	0.00	0.00	\N	new	2025-09-17 18:55:47.959985
64	8	0.00	0.00	\N	new	2025-09-17 18:55:47.959985
65	1	389.98	2.50	2025-09-10	regular	2025-09-18 10:49:51.015695
66	2	349.49	1.80	2025-09-08	regular	2025-09-18 10:49:51.015695
67	3	549.99	1.20	2025-09-07	high_value	2025-09-18 10:49:51.015695
68	4	79.99	0.50	2025-09-05	new	2025-09-18 10:49:51.015695
69	5	349.99	1.00	2025-09-09	regular	2025-09-18 10:49:51.015695
70	6	129.99	0.80	2025-09-06	new	2025-09-18 10:49:51.015695
71	7	0.00	0.00	\N	new	2025-09-18 10:49:51.015695
72	8	0.00	0.00	\N	new	2025-09-18 10:49:51.015695
73	1	389.98	2.50	2025-09-10	regular	2025-09-18 10:59:49.001578
74	2	349.49	1.80	2025-09-08	regular	2025-09-18 10:59:49.001578
75	3	549.99	1.20	2025-09-07	high_value	2025-09-18 10:59:49.001578
76	4	79.99	0.50	2025-09-05	new	2025-09-18 10:59:49.001578
77	5	349.99	1.00	2025-09-09	regular	2025-09-18 10:59:49.001578
78	6	129.99	0.80	2025-09-06	new	2025-09-18 10:59:49.001578
79	7	0.00	0.00	\N	new	2025-09-18 10:59:49.001578
80	8	0.00	0.00	\N	new	2025-09-18 10:59:49.001578
81	1	389.98	2.50	2025-09-10	regular	2025-09-18 11:28:06.712985
82	2	349.49	1.80	2025-09-08	regular	2025-09-18 11:28:06.712985
83	3	549.99	1.20	2025-09-07	high_value	2025-09-18 11:28:06.712985
84	4	79.99	0.50	2025-09-05	new	2025-09-18 11:28:06.712985
85	5	349.99	1.00	2025-09-09	regular	2025-09-18 11:28:06.712985
86	6	129.99	0.80	2025-09-06	new	2025-09-18 11:28:06.712985
87	7	0.00	0.00	\N	new	2025-09-18 11:28:06.712985
88	8	0.00	0.00	\N	new	2025-09-18 11:28:06.712985
89	1	389.98	2.50	2025-09-10	regular	2025-09-18 11:51:37.457976
90	2	349.49	1.80	2025-09-08	regular	2025-09-18 11:51:37.457976
91	3	549.99	1.20	2025-09-07	high_value	2025-09-18 11:51:37.457976
92	4	79.99	0.50	2025-09-05	new	2025-09-18 11:51:37.457976
93	5	349.99	1.00	2025-09-09	regular	2025-09-18 11:51:37.457976
94	6	129.99	0.80	2025-09-06	new	2025-09-18 11:51:37.457976
95	7	0.00	0.00	\N	new	2025-09-18 11:51:37.457976
96	8	0.00	0.00	\N	new	2025-09-18 11:51:37.457976
97	1	389.98	2.50	2025-09-10	regular	2025-09-18 11:59:28.720193
98	2	349.49	1.80	2025-09-08	regular	2025-09-18 11:59:28.720193
99	3	549.99	1.20	2025-09-07	high_value	2025-09-18 11:59:28.720193
100	4	79.99	0.50	2025-09-05	new	2025-09-18 11:59:28.720193
101	5	349.99	1.00	2025-09-09	regular	2025-09-18 11:59:28.720193
102	6	129.99	0.80	2025-09-06	new	2025-09-18 11:59:28.720193
103	7	0.00	0.00	\N	new	2025-09-18 11:59:28.720193
104	8	0.00	0.00	\N	new	2025-09-18 11:59:28.720193
\.


--
-- TOC entry 3703 (class 0 OID 33763)
-- Dependencies: 222
-- Data for Name: product_analytics; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.product_analytics (product_analytics_id, product_sku, total_units_sold, total_revenue, avg_selling_price, last_sale_date, trend, analysis_date) FROM stdin;
1	LAP-001	15	13499.85	899.99	2025-09-10	stable	2025-09-13
2	MOU-001	45	1349.55	29.99	2025-09-09	increasing	2025-09-13
3	DES-001	8	2799.92	349.99	2025-09-08	stable	2025-09-13
4	MON-001	12	2399.88	199.99	2025-09-07	increasing	2025-09-13
5	KEY-001	18	2339.82	129.99	2025-09-09	stable	2025-09-13
6	CHR-001	5	1499.95	299.99	2025-09-06	decreasing	2025-09-13
7	CAM-001	22	1759.78	79.99	2025-09-10	increasing	2025-09-13
8	HED-001	9	1799.91	199.99	2025-09-08	stable	2025-09-13
9	LAP-001	15	13499.85	899.99	2025-09-10	stable	2025-09-13
10	MOU-001	45	1349.55	29.99	2025-09-09	increasing	2025-09-13
11	DES-001	8	2799.92	349.99	2025-09-08	stable	2025-09-13
12	MON-001	12	2399.88	199.99	2025-09-07	increasing	2025-09-13
13	KEY-001	18	2339.82	129.99	2025-09-09	stable	2025-09-13
14	CHR-001	5	1499.95	299.99	2025-09-06	decreasing	2025-09-13
15	CAM-001	22	1759.78	79.99	2025-09-10	increasing	2025-09-13
16	HED-001	9	1799.91	199.99	2025-09-08	stable	2025-09-13
17	LAP-001	15	13499.85	899.99	2025-09-10	stable	2025-09-13
18	MOU-001	45	1349.55	29.99	2025-09-09	increasing	2025-09-13
19	DES-001	8	2799.92	349.99	2025-09-08	stable	2025-09-13
20	MON-001	12	2399.88	199.99	2025-09-07	increasing	2025-09-13
21	KEY-001	18	2339.82	129.99	2025-09-09	stable	2025-09-13
22	CHR-001	5	1499.95	299.99	2025-09-06	decreasing	2025-09-13
23	CAM-001	22	1759.78	79.99	2025-09-10	increasing	2025-09-13
24	HED-001	9	1799.91	199.99	2025-09-08	stable	2025-09-13
25	LAP-001	15	13499.85	899.99	2025-09-10	stable	2025-09-13
26	MOU-001	45	1349.55	29.99	2025-09-09	increasing	2025-09-13
27	DES-001	8	2799.92	349.99	2025-09-08	stable	2025-09-13
28	MON-001	12	2399.88	199.99	2025-09-07	increasing	2025-09-13
29	KEY-001	18	2339.82	129.99	2025-09-09	stable	2025-09-13
30	CHR-001	5	1499.95	299.99	2025-09-06	decreasing	2025-09-13
31	CAM-001	22	1759.78	79.99	2025-09-10	increasing	2025-09-13
32	HED-001	9	1799.91	199.99	2025-09-08	stable	2025-09-13
33	LAP-001	15	13499.85	899.99	2025-09-10	stable	2025-09-15
34	MOU-001	45	1349.55	29.99	2025-09-09	increasing	2025-09-15
35	DES-001	8	2799.92	349.99	2025-09-08	stable	2025-09-15
36	MON-001	12	2399.88	199.99	2025-09-07	increasing	2025-09-15
37	KEY-001	18	2339.82	129.99	2025-09-09	stable	2025-09-15
38	CHR-001	5	1499.95	299.99	2025-09-06	decreasing	2025-09-15
39	CAM-001	22	1759.78	79.99	2025-09-10	increasing	2025-09-15
40	HED-001	9	1799.91	199.99	2025-09-08	stable	2025-09-15
41	LAP-001	15	13499.85	899.99	2025-09-10	stable	2025-09-15
42	MOU-001	45	1349.55	29.99	2025-09-09	increasing	2025-09-15
43	DES-001	8	2799.92	349.99	2025-09-08	stable	2025-09-15
44	MON-001	12	2399.88	199.99	2025-09-07	increasing	2025-09-15
45	KEY-001	18	2339.82	129.99	2025-09-09	stable	2025-09-15
46	CHR-001	5	1499.95	299.99	2025-09-06	decreasing	2025-09-15
47	CAM-001	22	1759.78	79.99	2025-09-10	increasing	2025-09-15
48	HED-001	9	1799.91	199.99	2025-09-08	stable	2025-09-15
49	LAP-001	15	13499.85	899.99	2025-09-10	stable	2025-09-15
50	MOU-001	45	1349.55	29.99	2025-09-09	increasing	2025-09-15
51	DES-001	8	2799.92	349.99	2025-09-08	stable	2025-09-15
52	MON-001	12	2399.88	199.99	2025-09-07	increasing	2025-09-15
53	KEY-001	18	2339.82	129.99	2025-09-09	stable	2025-09-15
54	CHR-001	5	1499.95	299.99	2025-09-06	decreasing	2025-09-15
55	CAM-001	22	1759.78	79.99	2025-09-10	increasing	2025-09-15
56	HED-001	9	1799.91	199.99	2025-09-08	stable	2025-09-15
57	LAP-001	15	13499.85	899.99	2025-09-10	stable	2025-09-17
58	MOU-001	45	1349.55	29.99	2025-09-09	increasing	2025-09-17
59	DES-001	8	2799.92	349.99	2025-09-08	stable	2025-09-17
60	MON-001	12	2399.88	199.99	2025-09-07	increasing	2025-09-17
61	KEY-001	18	2339.82	129.99	2025-09-09	stable	2025-09-17
62	CHR-001	5	1499.95	299.99	2025-09-06	decreasing	2025-09-17
63	CAM-001	22	1759.78	79.99	2025-09-10	increasing	2025-09-17
64	HED-001	9	1799.91	199.99	2025-09-08	stable	2025-09-17
65	LAP-001	15	13499.85	899.99	2025-09-10	stable	2025-09-18
66	MOU-001	45	1349.55	29.99	2025-09-09	increasing	2025-09-18
67	DES-001	8	2799.92	349.99	2025-09-08	stable	2025-09-18
68	MON-001	12	2399.88	199.99	2025-09-07	increasing	2025-09-18
69	KEY-001	18	2339.82	129.99	2025-09-09	stable	2025-09-18
70	CHR-001	5	1499.95	299.99	2025-09-06	decreasing	2025-09-18
71	CAM-001	22	1759.78	79.99	2025-09-10	increasing	2025-09-18
72	HED-001	9	1799.91	199.99	2025-09-08	stable	2025-09-18
73	LAP-001	15	13499.85	899.99	2025-09-10	stable	2025-09-18
74	MOU-001	45	1349.55	29.99	2025-09-09	increasing	2025-09-18
75	DES-001	8	2799.92	349.99	2025-09-08	stable	2025-09-18
76	MON-001	12	2399.88	199.99	2025-09-07	increasing	2025-09-18
77	KEY-001	18	2339.82	129.99	2025-09-09	stable	2025-09-18
78	CHR-001	5	1499.95	299.99	2025-09-06	decreasing	2025-09-18
79	CAM-001	22	1759.78	79.99	2025-09-10	increasing	2025-09-18
80	HED-001	9	1799.91	199.99	2025-09-08	stable	2025-09-18
81	LAP-001	15	13499.85	899.99	2025-09-10	stable	2025-09-18
82	MOU-001	45	1349.55	29.99	2025-09-09	increasing	2025-09-18
83	DES-001	8	2799.92	349.99	2025-09-08	stable	2025-09-18
84	MON-001	12	2399.88	199.99	2025-09-07	increasing	2025-09-18
85	KEY-001	18	2339.82	129.99	2025-09-09	stable	2025-09-18
86	CHR-001	5	1499.95	299.99	2025-09-06	decreasing	2025-09-18
87	CAM-001	22	1759.78	79.99	2025-09-10	increasing	2025-09-18
88	HED-001	9	1799.91	199.99	2025-09-08	stable	2025-09-18
89	LAP-001	15	13499.85	899.99	2025-09-10	stable	2025-09-18
90	MOU-001	45	1349.55	29.99	2025-09-09	increasing	2025-09-18
91	DES-001	8	2799.92	349.99	2025-09-08	stable	2025-09-18
92	MON-001	12	2399.88	199.99	2025-09-07	increasing	2025-09-18
93	KEY-001	18	2339.82	129.99	2025-09-09	stable	2025-09-18
94	CHR-001	5	1499.95	299.99	2025-09-06	decreasing	2025-09-18
95	CAM-001	22	1759.78	79.99	2025-09-10	increasing	2025-09-18
96	HED-001	9	1799.91	199.99	2025-09-08	stable	2025-09-18
97	LAP-001	15	13499.85	899.99	2025-09-10	stable	2025-09-18
98	MOU-001	45	1349.55	29.99	2025-09-09	increasing	2025-09-18
99	DES-001	8	2799.92	349.99	2025-09-08	stable	2025-09-18
100	MON-001	12	2399.88	199.99	2025-09-07	increasing	2025-09-18
101	KEY-001	18	2339.82	129.99	2025-09-09	stable	2025-09-18
102	CHR-001	5	1499.95	299.99	2025-09-06	decreasing	2025-09-18
103	CAM-001	22	1759.78	79.99	2025-09-10	increasing	2025-09-18
104	HED-001	9	1799.91	199.99	2025-09-08	stable	2025-09-18
\.


--
-- TOC entry 3699 (class 0 OID 33741)
-- Dependencies: 218
-- Data for Name: sales_metrics; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.sales_metrics (metric_id, date_recorded, total_sales, order_count, avg_order_value, top_selling_category, created_at) FROM stdin;
2	2025-09-02	1890.50	9	210.06	Electronics	2025-09-13 10:50:00.660438
3	2025-09-03	3200.25	15	213.35	Furniture	2025-09-13 10:50:00.660438
4	2025-09-04	1650.00	8	206.25	Electronics	2025-09-13 10:50:00.660438
5	2025-09-05	2890.75	14	206.48	Electronics	2025-09-13 10:50:00.660438
6	2025-09-06	2150.50	11	195.50	Electronics	2025-09-13 10:50:00.660438
7	2025-09-07	1750.25	7	250.04	Furniture	2025-09-13 10:50:00.660438
8	2025-09-08	2950.00	13	226.92	Electronics	2025-09-13 10:50:00.660438
9	2025-09-09	2100.75	10	210.08	Electronics	2025-09-13 10:50:00.660438
10	2025-09-10	3450.50	16	215.66	Electronics	2025-09-13 10:50:00.660438
11	2025-09-01	2450.75	12	204.23	Electronics	2025-09-13 21:14:52.926033
12	2025-09-02	1890.50	9	210.06	Electronics	2025-09-13 21:14:52.926033
13	2025-09-03	3200.25	15	213.35	Furniture	2025-09-13 21:14:52.926033
14	2025-09-04	1650.00	8	206.25	Electronics	2025-09-13 21:14:52.926033
15	2025-09-05	2890.75	14	206.48	Electronics	2025-09-13 21:14:52.926033
16	2025-09-06	2150.50	11	195.50	Electronics	2025-09-13 21:14:52.926033
17	2025-09-07	1750.25	7	250.04	Furniture	2025-09-13 21:14:52.926033
18	2025-09-08	2950.00	13	226.92	Electronics	2025-09-13 21:14:52.926033
19	2025-09-09	2100.75	10	210.08	Electronics	2025-09-13 21:14:52.926033
20	2025-09-10	3450.50	16	215.66	Electronics	2025-09-13 21:14:52.926033
21	2025-09-01	2450.75	12	204.23	Electronics	2025-09-13 22:06:21.616528
22	2025-09-02	1890.50	9	210.06	Electronics	2025-09-13 22:06:21.616528
23	2025-09-03	3200.25	15	213.35	Furniture	2025-09-13 22:06:21.616528
24	2025-09-04	1650.00	8	206.25	Electronics	2025-09-13 22:06:21.616528
25	2025-09-05	2890.75	14	206.48	Electronics	2025-09-13 22:06:21.616528
26	2025-09-06	2150.50	11	195.50	Electronics	2025-09-13 22:06:21.616528
27	2025-09-07	1750.25	7	250.04	Furniture	2025-09-13 22:06:21.616528
28	2025-09-08	2950.00	13	226.92	Electronics	2025-09-13 22:06:21.616528
29	2025-09-09	2100.75	10	210.08	Electronics	2025-09-13 22:06:21.616528
30	2025-09-10	3450.50	16	215.66	Electronics	2025-09-13 22:06:21.616528
31	2025-09-01	2450.75	12	204.23	Electronics	2025-09-13 22:33:46.160819
32	2025-09-02	1890.50	9	210.06	Electronics	2025-09-13 22:33:46.160819
33	2025-09-03	3200.25	15	213.35	Furniture	2025-09-13 22:33:46.160819
34	2025-09-04	1650.00	8	206.25	Electronics	2025-09-13 22:33:46.160819
35	2025-09-05	2890.75	14	206.48	Electronics	2025-09-13 22:33:46.160819
36	2025-09-06	2150.50	11	195.50	Electronics	2025-09-13 22:33:46.160819
37	2025-09-07	1750.25	7	250.04	Furniture	2025-09-13 22:33:46.160819
38	2025-09-08	2950.00	13	226.92	Electronics	2025-09-13 22:33:46.160819
39	2025-09-09	2100.75	10	210.08	Electronics	2025-09-13 22:33:46.160819
40	2025-09-10	3450.50	16	215.66	Electronics	2025-09-13 22:33:46.160819
41	2025-09-01	2450.75	12	204.23	Electronics	2025-09-15 12:07:12.98907
42	2025-09-02	1890.50	9	210.06	Electronics	2025-09-15 12:07:12.98907
43	2025-09-03	3200.25	15	213.35	Furniture	2025-09-15 12:07:12.98907
44	2025-09-04	1650.00	8	206.25	Electronics	2025-09-15 12:07:12.98907
45	2025-09-05	2890.75	14	206.48	Electronics	2025-09-15 12:07:12.98907
46	2025-09-06	2150.50	11	195.50	Electronics	2025-09-15 12:07:12.98907
47	2025-09-07	1750.25	7	250.04	Furniture	2025-09-15 12:07:12.98907
48	2025-09-08	2950.00	13	226.92	Electronics	2025-09-15 12:07:12.98907
49	2025-09-09	2100.75	10	210.08	Electronics	2025-09-15 12:07:12.98907
50	2025-09-10	3450.50	16	215.66	Electronics	2025-09-15 12:07:12.98907
51	2025-09-01	2450.75	12	204.23	Electronics	2025-09-15 12:29:22.024786
52	2025-09-02	1890.50	9	210.06	Electronics	2025-09-15 12:29:22.024786
53	2025-09-03	3200.25	15	213.35	Furniture	2025-09-15 12:29:22.024786
54	2025-09-04	1650.00	8	206.25	Electronics	2025-09-15 12:29:22.024786
55	2025-09-05	2890.75	14	206.48	Electronics	2025-09-15 12:29:22.024786
56	2025-09-06	2150.50	11	195.50	Electronics	2025-09-15 12:29:22.024786
57	2025-09-07	1750.25	7	250.04	Furniture	2025-09-15 12:29:22.024786
58	2025-09-08	2950.00	13	226.92	Electronics	2025-09-15 12:29:22.024786
59	2025-09-09	2100.75	10	210.08	Electronics	2025-09-15 12:29:22.024786
60	2025-09-10	3450.50	16	215.66	Electronics	2025-09-15 12:29:22.024786
61	2025-09-01	2450.75	12	204.23	Electronics	2025-09-15 15:17:19.274725
62	2025-09-02	1890.50	9	210.06	Electronics	2025-09-15 15:17:19.274725
63	2025-09-03	3200.25	15	213.35	Furniture	2025-09-15 15:17:19.274725
64	2025-09-04	1650.00	8	206.25	Electronics	2025-09-15 15:17:19.274725
65	2025-09-05	2890.75	14	206.48	Electronics	2025-09-15 15:17:19.274725
66	2025-09-06	2150.50	11	195.50	Electronics	2025-09-15 15:17:19.274725
67	2025-09-07	1750.25	7	250.04	Furniture	2025-09-15 15:17:19.274725
68	2025-09-08	2950.00	13	226.92	Electronics	2025-09-15 15:17:19.274725
69	2025-09-09	2100.75	10	210.08	Electronics	2025-09-15 15:17:19.274725
70	2025-09-10	3450.50	16	215.66	Electronics	2025-09-15 15:17:19.274725
71	2025-09-01	2450.75	12	204.23	Electronics	2025-09-17 18:55:47.958788
72	2025-09-02	1890.50	9	210.06	Electronics	2025-09-17 18:55:47.958788
73	2025-09-03	3200.25	15	213.35	Furniture	2025-09-17 18:55:47.958788
74	2025-09-04	1650.00	8	206.25	Electronics	2025-09-17 18:55:47.958788
75	2025-09-05	2890.75	14	206.48	Electronics	2025-09-17 18:55:47.958788
76	2025-09-06	2150.50	11	195.50	Electronics	2025-09-17 18:55:47.958788
77	2025-09-07	1750.25	7	250.04	Furniture	2025-09-17 18:55:47.958788
78	2025-09-08	2950.00	13	226.92	Electronics	2025-09-17 18:55:47.958788
79	2025-09-09	2100.75	10	210.08	Electronics	2025-09-17 18:55:47.958788
80	2025-09-10	3450.50	16	215.66	Electronics	2025-09-17 18:55:47.958788
81	2025-09-01	2450.75	12	204.23	Electronics	2025-09-18 10:49:51.011438
82	2025-09-02	1890.50	9	210.06	Electronics	2025-09-18 10:49:51.011438
83	2025-09-03	3200.25	15	213.35	Furniture	2025-09-18 10:49:51.011438
84	2025-09-04	1650.00	8	206.25	Electronics	2025-09-18 10:49:51.011438
85	2025-09-05	2890.75	14	206.48	Electronics	2025-09-18 10:49:51.011438
86	2025-09-06	2150.50	11	195.50	Electronics	2025-09-18 10:49:51.011438
87	2025-09-07	1750.25	7	250.04	Furniture	2025-09-18 10:49:51.011438
88	2025-09-08	2950.00	13	226.92	Electronics	2025-09-18 10:49:51.011438
89	2025-09-09	2100.75	10	210.08	Electronics	2025-09-18 10:49:51.011438
90	2025-09-10	3450.50	16	215.66	Electronics	2025-09-18 10:49:51.011438
91	2025-09-01	2450.75	12	204.23	Electronics	2025-09-18 10:59:49.000542
92	2025-09-02	1890.50	9	210.06	Electronics	2025-09-18 10:59:49.000542
93	2025-09-03	3200.25	15	213.35	Furniture	2025-09-18 10:59:49.000542
94	2025-09-04	1650.00	8	206.25	Electronics	2025-09-18 10:59:49.000542
95	2025-09-05	2890.75	14	206.48	Electronics	2025-09-18 10:59:49.000542
96	2025-09-06	2150.50	11	195.50	Electronics	2025-09-18 10:59:49.000542
97	2025-09-07	1750.25	7	250.04	Furniture	2025-09-18 10:59:49.000542
98	2025-09-08	2950.00	13	226.92	Electronics	2025-09-18 10:59:49.000542
99	2025-09-09	2100.75	10	210.08	Electronics	2025-09-18 10:59:49.000542
100	2025-09-10	3450.50	16	215.66	Electronics	2025-09-18 10:59:49.000542
101	2025-09-01	2450.75	12	204.23	Electronics	2025-09-18 11:28:06.712114
102	2025-09-02	1890.50	9	210.06	Electronics	2025-09-18 11:28:06.712114
103	2025-09-03	3200.25	15	213.35	Furniture	2025-09-18 11:28:06.712114
104	2025-09-04	1650.00	8	206.25	Electronics	2025-09-18 11:28:06.712114
105	2025-09-05	2890.75	14	206.48	Electronics	2025-09-18 11:28:06.712114
106	2025-09-06	2150.50	11	195.50	Electronics	2025-09-18 11:28:06.712114
107	2025-09-07	1750.25	7	250.04	Furniture	2025-09-18 11:28:06.712114
108	2025-09-08	2950.00	13	226.92	Electronics	2025-09-18 11:28:06.712114
109	2025-09-09	2100.75	10	210.08	Electronics	2025-09-18 11:28:06.712114
110	2025-09-10	3450.50	16	215.66	Electronics	2025-09-18 11:28:06.712114
111	2025-09-01	2450.75	12	204.23	Electronics	2025-09-18 11:51:37.457121
112	2025-09-02	1890.50	9	210.06	Electronics	2025-09-18 11:51:37.457121
113	2025-09-03	3200.25	15	213.35	Furniture	2025-09-18 11:51:37.457121
114	2025-09-04	1650.00	8	206.25	Electronics	2025-09-18 11:51:37.457121
115	2025-09-05	2890.75	14	206.48	Electronics	2025-09-18 11:51:37.457121
116	2025-09-06	2150.50	11	195.50	Electronics	2025-09-18 11:51:37.457121
117	2025-09-07	1750.25	7	250.04	Furniture	2025-09-18 11:51:37.457121
118	2025-09-08	2950.00	13	226.92	Electronics	2025-09-18 11:51:37.457121
119	2025-09-09	2100.75	10	210.08	Electronics	2025-09-18 11:51:37.457121
120	2025-09-10	3450.50	16	215.66	Electronics	2025-09-18 11:51:37.457121
121	2025-09-01	2450.75	12	204.23	Electronics	2025-09-18 11:59:28.719434
122	2025-09-02	1890.50	9	210.06	Electronics	2025-09-18 11:59:28.719434
123	2025-09-03	3200.25	15	213.35	Furniture	2025-09-18 11:59:28.719434
124	2025-09-04	1650.00	8	206.25	Electronics	2025-09-18 11:59:28.719434
125	2025-09-05	2890.75	14	206.48	Electronics	2025-09-18 11:59:28.719434
126	2025-09-06	2150.50	11	195.50	Electronics	2025-09-18 11:59:28.719434
127	2025-09-07	1750.25	7	250.04	Furniture	2025-09-18 11:59:28.719434
128	2025-09-08	2950.00	13	226.92	Electronics	2025-09-18 11:59:28.719434
129	2025-09-09	2100.75	10	210.08	Electronics	2025-09-18 11:59:28.719434
130	2025-09-10	3450.50	16	215.66	Electronics	2025-09-18 11:59:28.719434
1	2025-09-01	0.00	12	204.23	Electronics	2025-09-13 10:50:00.660438
\.


--
-- TOC entry 3712 (class 0 OID 0)
-- Dependencies: 219
-- Name: customer_analytics_analytics_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.customer_analytics_analytics_id_seq', 104, true);


--
-- TOC entry 3713 (class 0 OID 0)
-- Dependencies: 221
-- Name: product_analytics_product_analytics_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.product_analytics_product_analytics_id_seq', 104, true);


--
-- TOC entry 3714 (class 0 OID 0)
-- Dependencies: 217
-- Name: sales_metrics_metric_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.sales_metrics_metric_id_seq', 130, true);


--
-- TOC entry 3546 (class 2606 OID 33761)
-- Name: customer_analytics customer_analytics_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.customer_analytics
    ADD CONSTRAINT customer_analytics_pkey PRIMARY KEY (analytics_id);


--
-- TOC entry 3552 (class 2606 OID 33771)
-- Name: product_analytics product_analytics_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.product_analytics
    ADD CONSTRAINT product_analytics_pkey PRIMARY KEY (product_analytics_id);


--
-- TOC entry 3544 (class 2606 OID 33750)
-- Name: sales_metrics sales_metrics_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.sales_metrics
    ADD CONSTRAINT sales_metrics_pkey PRIMARY KEY (metric_id);


--
-- TOC entry 3547 (class 1259 OID 33773)
-- Name: idx_customer_analytics_customer_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_customer_analytics_customer_id ON public.customer_analytics USING btree (customer_id);


--
-- TOC entry 3548 (class 1259 OID 33774)
-- Name: idx_customer_analytics_segment; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_customer_analytics_segment ON public.customer_analytics USING btree (customer_segment);


--
-- TOC entry 3549 (class 1259 OID 33776)
-- Name: idx_product_analytics_date; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_product_analytics_date ON public.product_analytics USING btree (analysis_date);


--
-- TOC entry 3550 (class 1259 OID 33775)
-- Name: idx_product_analytics_sku; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_product_analytics_sku ON public.product_analytics USING btree (product_sku);


--
-- TOC entry 3542 (class 1259 OID 33772)
-- Name: idx_sales_metrics_date; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_sales_metrics_date ON public.sales_metrics USING btree (date_recorded);


-- Completed on 2025-09-22 12:58:50 IST

--
-- PostgreSQL database dump complete
--

\unrestrict FcqGudY7sztGoEyH3JpMDqg7fxZnz9n9PiHVuGNF6GQfhSmSDdr2f9ADScVvggN

