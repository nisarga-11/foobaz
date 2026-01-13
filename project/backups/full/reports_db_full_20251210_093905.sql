--
-- PostgreSQL database dump
--

\restrict Ej39y3am9KdOWxAygawvW3f1WMHc1qt06EnbNUXShUbSOpVg1eIbjOLBgf5GCIK

-- Dumped from database version 17.6
-- Dumped by pg_dump version 17.6

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
-- Name: daily_sales; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.daily_sales (
    sale_id integer NOT NULL,
    sale_date date,
    total_orders integer,
    total_revenue numeric(12,2)
);


ALTER TABLE public.daily_sales OWNER TO postgres;

--
-- Name: daily_sales_sale_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.daily_sales_sale_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.daily_sales_sale_id_seq OWNER TO postgres;

--
-- Name: daily_sales_sale_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.daily_sales_sale_id_seq OWNED BY public.daily_sales.sale_id;


--
-- Name: traffic_stats; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.traffic_stats (
    stat_id integer NOT NULL,
    visit_date date,
    page_views integer,
    unique_visitors integer
);


ALTER TABLE public.traffic_stats OWNER TO postgres;

--
-- Name: traffic_stats_stat_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.traffic_stats_stat_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.traffic_stats_stat_id_seq OWNER TO postgres;

--
-- Name: traffic_stats_stat_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.traffic_stats_stat_id_seq OWNED BY public.traffic_stats.stat_id;


--
-- Name: daily_sales sale_id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.daily_sales ALTER COLUMN sale_id SET DEFAULT nextval('public.daily_sales_sale_id_seq'::regclass);


--
-- Name: traffic_stats stat_id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.traffic_stats ALTER COLUMN stat_id SET DEFAULT nextval('public.traffic_stats_stat_id_seq'::regclass);


--
-- Data for Name: daily_sales; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.daily_sales (sale_id, sale_date, total_orders, total_revenue) FROM stdin;
1	2025-12-08	20	58000.00
2	2025-12-09	32	96400.50
\.


--
-- Data for Name: traffic_stats; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.traffic_stats (stat_id, visit_date, page_views, unique_visitors) FROM stdin;
1	2025-12-08	1500	420
2	2025-12-09	2100	610
\.


--
-- Name: daily_sales_sale_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.daily_sales_sale_id_seq', 2, true);


--
-- Name: traffic_stats_stat_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.traffic_stats_stat_id_seq', 2, true);


--
-- Name: daily_sales daily_sales_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.daily_sales
    ADD CONSTRAINT daily_sales_pkey PRIMARY KEY (sale_id);


--
-- Name: traffic_stats traffic_stats_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.traffic_stats
    ADD CONSTRAINT traffic_stats_pkey PRIMARY KEY (stat_id);


--
-- PostgreSQL database dump complete
--

\unrestrict Ej39y3am9KdOWxAygawvW3f1WMHc1qt06EnbNUXShUbSOpVg1eIbjOLBgf5GCIK

