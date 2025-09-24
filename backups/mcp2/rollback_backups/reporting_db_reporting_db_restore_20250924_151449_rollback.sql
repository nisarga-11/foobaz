--
-- PostgreSQL database dump
--

\restrict p4sSyuxK311LOAdXVxpO0xFw0NDDT8yIdxhtMVidHeC1VCPqNSMRnJ3KaGEoDZh

-- Dumped from database version 17.6 (Postgres.app)
-- Dumped by pg_dump version 17.6 (Postgres.app)

-- Started on 2025-09-24 15:14:49 IST

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
-- TOC entry 218 (class 1259 OID 33967)
-- Name: daily_reports; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.daily_reports (
    report_id integer NOT NULL,
    report_date date NOT NULL,
    total_sales numeric(12,2) DEFAULT 0.00,
    new_customers integer DEFAULT 0,
    active_employees integer DEFAULT 0,
    cash_balance numeric(15,2) DEFAULT 0.00,
    inventory_value numeric(12,2) DEFAULT 0.00,
    orders_count integer DEFAULT 0,
    avg_order_value numeric(8,2) DEFAULT 0.00,
    generated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


ALTER TABLE public.daily_reports OWNER TO postgres;

--
-- TOC entry 217 (class 1259 OID 33966)
-- Name: daily_reports_report_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.daily_reports_report_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.daily_reports_report_id_seq OWNER TO postgres;

--
-- TOC entry 3709 (class 0 OID 0)
-- Dependencies: 217
-- Name: daily_reports_report_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.daily_reports_report_id_seq OWNED BY public.daily_reports.report_id;


--
-- TOC entry 222 (class 1259 OID 33994)
-- Name: executive_dashboard; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.executive_dashboard (
    dashboard_id integer NOT NULL,
    metric_category character varying(50) NOT NULL,
    metric_name character varying(100) NOT NULL,
    current_value numeric(15,4),
    previous_value numeric(15,4),
    change_percentage numeric(5,2),
    trend character varying(20),
    update_date date NOT NULL,
    data_source character varying(50)
);


ALTER TABLE public.executive_dashboard OWNER TO postgres;

--
-- TOC entry 221 (class 1259 OID 33993)
-- Name: executive_dashboard_dashboard_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.executive_dashboard_dashboard_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.executive_dashboard_dashboard_id_seq OWNER TO postgres;

--
-- TOC entry 3710 (class 0 OID 0)
-- Dependencies: 221
-- Name: executive_dashboard_dashboard_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.executive_dashboard_dashboard_id_seq OWNED BY public.executive_dashboard.dashboard_id;


--
-- TOC entry 220 (class 1259 OID 33984)
-- Name: kpi_tracking; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.kpi_tracking (
    kpi_id integer NOT NULL,
    kpi_name character varying(100) NOT NULL,
    kpi_value numeric(12,4) NOT NULL,
    target_value numeric(12,4),
    measurement_period character varying(20),
    measurement_date date NOT NULL,
    department character varying(50),
    notes text,
    CONSTRAINT valid_period CHECK (((measurement_period)::text = ANY ((ARRAY['daily'::character varying, 'weekly'::character varying, 'monthly'::character varying, 'quarterly'::character varying, 'yearly'::character varying])::text[])))
);


ALTER TABLE public.kpi_tracking OWNER TO postgres;

--
-- TOC entry 219 (class 1259 OID 33983)
-- Name: kpi_tracking_kpi_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.kpi_tracking_kpi_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.kpi_tracking_kpi_id_seq OWNER TO postgres;

--
-- TOC entry 3711 (class 0 OID 0)
-- Dependencies: 219
-- Name: kpi_tracking_kpi_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.kpi_tracking_kpi_id_seq OWNED BY public.kpi_tracking.kpi_id;


--
-- TOC entry 3528 (class 2604 OID 33970)
-- Name: daily_reports report_id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.daily_reports ALTER COLUMN report_id SET DEFAULT nextval('public.daily_reports_report_id_seq'::regclass);


--
-- TOC entry 3538 (class 2604 OID 33997)
-- Name: executive_dashboard dashboard_id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.executive_dashboard ALTER COLUMN dashboard_id SET DEFAULT nextval('public.executive_dashboard_dashboard_id_seq'::regclass);


--
-- TOC entry 3537 (class 2604 OID 33987)
-- Name: kpi_tracking kpi_id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.kpi_tracking ALTER COLUMN kpi_id SET DEFAULT nextval('public.kpi_tracking_kpi_id_seq'::regclass);


--
-- TOC entry 3699 (class 0 OID 33967)
-- Dependencies: 218
-- Data for Name: daily_reports; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.daily_reports (report_id, report_date, total_sales, new_customers, active_employees, cash_balance, inventory_value, orders_count, avg_order_value, generated_at) FROM stdin;
2	2025-09-02	1890.50	1	10	323750.00	147250.00	9	210.06	2025-09-13 10:54:34.863531
3	2025-09-03	3200.25	3	10	326950.25	149750.25	15	213.35	2025-09-13 10:54:34.863531
4	2025-09-04	1650.00	0	10	325300.25	148100.25	8	206.25	2025-09-13 10:54:34.863531
5	2025-09-05	2890.75	2	10	328191.00	150641.00	14	206.48	2025-09-13 10:54:34.863531
6	2025-09-06	2150.50	1	10	330341.50	149791.50	11	195.50	2025-09-13 10:54:34.863531
7	2025-09-07	1750.25	1	10	332091.75	148541.75	7	250.04	2025-09-13 10:54:34.863531
8	2025-09-08	2950.00	2	10	335041.75	151491.75	13	226.92	2025-09-13 10:54:34.863531
9	2025-09-09	2100.75	1	10	337142.50	150342.50	10	210.08	2025-09-13 10:54:34.863531
10	2025-09-10	3450.50	3	10	340593.00	153793.00	16	215.66	2025-09-13 10:54:34.863531
1	2025-09-01	2450.75	222	10	325000.00	148500.00	12	204.23	2025-09-13 10:54:34.863531
\.


--
-- TOC entry 3703 (class 0 OID 33994)
-- Dependencies: 222
-- Data for Name: executive_dashboard; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.executive_dashboard (dashboard_id, metric_category, metric_name, current_value, previous_value, change_percentage, trend, update_date, data_source) FROM stdin;
1	Financial	Monthly Revenue	235000.0000	220000.0000	6.82	up	2025-09-10	finance_db
2	Financial	Cash Position	340593.0000	325000.0000	4.80	up	2025-09-10	finance_db
3	Financial	Gross Margin %	45.8000	43.2000	6.02	up	2025-09-10	analytics_db
4	Operations	Inventory Value	153793.0000	148500.0000	3.57	up	2025-09-10	inventory_db
5	Operations	Order Fulfillment Rate	98.5000	97.2000	1.34	up	2025-09-10	customer_db
6	Sales	New Customers	16.0000	12.0000	33.33	up	2025-09-10	customer_db
7	Sales	Average Order Value	215.6600	204.2300	5.60	up	2025-09-10	customer_db
8	HR	Active Employees	10.0000	10.0000	0.00	stable	2025-09-10	hr_db
9	HR	Employee Utilization	92.5000	89.0000	3.93	up	2025-09-10	hr_db
10	Engineering	System Uptime %	99.8000	99.2000	0.60	up	2025-09-10	monitoring
\.


--
-- TOC entry 3701 (class 0 OID 33984)
-- Dependencies: 220
-- Data for Name: kpi_tracking; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.kpi_tracking (kpi_id, kpi_name, kpi_value, target_value, measurement_period, measurement_date, department, notes) FROM stdin;
1	Customer Acquisition Cost	125.5000	100.0000	monthly	2025-09-01	Marketing	Slightly above target
2	Employee Satisfaction Score	4.2000	4.0000	quarterly	2025-09-01	HR	Above target, good progress
3	Revenue Growth Rate	12.5000	15.0000	monthly	2025-09-01	Sales	Below target, needs attention
4	Inventory Turnover	8.2000	10.0000	monthly	2025-09-01	Operations	Below target
5	Gross Profit Margin	45.8000	50.0000	monthly	2025-09-01	Finance	Need to optimize costs
6	Customer Retention Rate	87.5000	85.0000	monthly	2025-09-01	Sales	Above target
7	Average Order Value	210.5000	200.0000	weekly	2025-09-08	Sales	Meeting target
8	Code Deployment Frequency	15.0000	12.0000	weekly	2025-09-08	Engineering	Above target
9	System Uptime	99.8000	99.5000	monthly	2025-09-01	Engineering	Excellent performance
10	Budget Variance	5.2000	5.0000	monthly	2025-09-01	Finance	Within acceptable range
\.


--
-- TOC entry 3712 (class 0 OID 0)
-- Dependencies: 217
-- Name: daily_reports_report_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.daily_reports_report_id_seq', 10, true);


--
-- TOC entry 3713 (class 0 OID 0)
-- Dependencies: 221
-- Name: executive_dashboard_dashboard_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.executive_dashboard_dashboard_id_seq', 10, true);


--
-- TOC entry 3714 (class 0 OID 0)
-- Dependencies: 219
-- Name: kpi_tracking_kpi_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.kpi_tracking_kpi_id_seq', 10, true);


--
-- TOC entry 3541 (class 2606 OID 33980)
-- Name: daily_reports daily_reports_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.daily_reports
    ADD CONSTRAINT daily_reports_pkey PRIMARY KEY (report_id);


--
-- TOC entry 3543 (class 2606 OID 33982)
-- Name: daily_reports daily_reports_report_date_key; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.daily_reports
    ADD CONSTRAINT daily_reports_report_date_key UNIQUE (report_date);


--
-- TOC entry 3550 (class 2606 OID 33999)
-- Name: executive_dashboard executive_dashboard_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.executive_dashboard
    ADD CONSTRAINT executive_dashboard_pkey PRIMARY KEY (dashboard_id);


--
-- TOC entry 3548 (class 2606 OID 33992)
-- Name: kpi_tracking kpi_tracking_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.kpi_tracking
    ADD CONSTRAINT kpi_tracking_pkey PRIMARY KEY (kpi_id);


--
-- TOC entry 3544 (class 1259 OID 34000)
-- Name: idx_daily_reports_date; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_daily_reports_date ON public.daily_reports USING btree (report_date);


--
-- TOC entry 3551 (class 1259 OID 34003)
-- Name: idx_executive_dashboard_category; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_executive_dashboard_category ON public.executive_dashboard USING btree (metric_category);


--
-- TOC entry 3552 (class 1259 OID 34004)
-- Name: idx_executive_dashboard_date; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_executive_dashboard_date ON public.executive_dashboard USING btree (update_date);


--
-- TOC entry 3545 (class 1259 OID 34001)
-- Name: idx_kpi_tracking_date; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_kpi_tracking_date ON public.kpi_tracking USING btree (measurement_date);


--
-- TOC entry 3546 (class 1259 OID 34002)
-- Name: idx_kpi_tracking_department; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_kpi_tracking_department ON public.kpi_tracking USING btree (department);


-- Completed on 2025-09-24 15:14:50 IST

--
-- PostgreSQL database dump complete
--

\unrestrict p4sSyuxK311LOAdXVxpO0xFw0NDDT8yIdxhtMVidHeC1VCPqNSMRnJ3KaGEoDZh

