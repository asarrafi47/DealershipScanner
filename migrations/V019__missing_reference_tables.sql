-- V019: missing reference/catalog tables that exist locally but were never added to prod schema.
CREATE EXTENSION IF NOT EXISTS vector;

CREATE FUNCTION public.refresh_vehicle_search_vector() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
  NEW.search_vector :=
    setweight(to_tsvector('english', COALESCE(NEW.make,        '')), 'A') ||
    setweight(to_tsvector('english', COALESCE(NEW.model,       '')), 'A') ||
    setweight(to_tsvector('english', COALESCE(NEW.trim,        '')), 'B') ||
    setweight(to_tsvector('english', COALESCE(NEW.body_style,  '')), 'C') ||
    setweight(to_tsvector('english', COALESCE(NEW.fuel_type,   '')), 'C') ||
    setweight(to_tsvector('english', COALESCE(NEW.engine_desc, '')), 'D') ||
    setweight(to_tsvector('english', COALESCE(NEW.notes,       '')), 'D');
  RETURN NEW;
END;
$$;

CREATE FUNCTION public.set_updated_at() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$;


--
-- Name: ai_engine_specs; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ai_engine_specs (
    year integer NOT NULL,
    make text NOT NULL,
    model text NOT NULL,
    engine_description text DEFAULT ''::text NOT NULL,
    cylinders integer,
    fuel_type text DEFAULT ''::text NOT NULL,
    horsepower integer,
    torque_lb_ft integer,
    tow_capacity_lb integer,
    zero_to_60_sec double precision,
    fuel_tank_gal double precision,
    curb_weight_lb integer,
    specs_json jsonb,
    source_host text DEFAULT 'ai-engine-research'::text,
    created_at timestamp with time zone DEFAULT now()
);


--
-- Name: ai_model_specs; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ai_model_specs (
    year integer NOT NULL,
    make text NOT NULL,
    model text NOT NULL,
    horsepower integer,
    torque_lb_ft integer,
    torque_nm integer,
    curb_weight_lb integer,
    curb_weight_kg integer,
    zero_to_60_sec double precision,
    fuel_tank_gal double precision,
    tow_capacity_lb integer,
    specs_json jsonb,
    source_host text DEFAULT 'ai-research'::text,
    created_at timestamp with time zone DEFAULT now()
);


--
-- Name: car_move_log; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.car_move_log (
    id bigint NOT NULL,
    car_id bigint NOT NULL,
    from_dealer_id text,
    from_dealer_name text,
    from_registry_id bigint,
    to_dealer_id text,
    to_dealer_name text,
    to_registry_id bigint,
    evidence text,
    moved_at timestamp with time zone DEFAULT now() NOT NULL,
    reverted_at timestamp with time zone
);


--
-- Name: car_move_log_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.car_move_log_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: car_move_log_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.car_move_log_id_seq OWNED BY public.car_move_log.id;


--
-- Name: cars_trim_quarantine; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.cars_trim_quarantine (
    car_id integer,
    vin text,
    old_trim text,
    provenance_source text,
    title text,
    trim_provenance_json text,
    quarantined_at text
);


--
-- Name: catalog_exterior_colors; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.catalog_exterior_colors (
    id bigint NOT NULL,
    vehicle_id bigint NOT NULL,
    color_name character varying(100) NOT NULL,
    color_code character varying(20),
    finish_type character varying(20),
    hex_code character(7),
    extra_cost integer DEFAULT 0 NOT NULL
);


--
-- Name: catalog_interior_colors; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.catalog_interior_colors (
    id bigint NOT NULL,
    vehicle_id bigint NOT NULL,
    color_name character varying(100) NOT NULL,
    color_code character varying(20),
    material character varying(30),
    hex_code character(7),
    extra_cost integer DEFAULT 0 NOT NULL
);


--
-- Name: catalog_options; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.catalog_options (
    id bigint NOT NULL,
    vehicle_id bigint NOT NULL,
    option_code character varying(20),
    option_name character varying(150) NOT NULL,
    option_msrp integer,
    category character varying(50),
    description text
);


--
-- Name: catalog_package_features; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.catalog_package_features (
    id bigint NOT NULL,
    package_id bigint NOT NULL,
    feature_name character varying(150) NOT NULL,
    feature_category character varying(50),
    sort_order smallint DEFAULT 0 NOT NULL
);


--
-- Name: catalog_packages; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.catalog_packages (
    id bigint NOT NULL,
    vehicle_id bigint NOT NULL,
    package_code character varying(20),
    package_name character varying(150) NOT NULL,
    package_msrp integer,
    is_required boolean DEFAULT false NOT NULL,
    sort_order smallint DEFAULT 0 NOT NULL,
    notes text
);


--
-- Name: catalog_trims; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.catalog_trims (
    id bigint NOT NULL,
    year smallint NOT NULL,
    make character varying(50) NOT NULL,
    model character varying(100) NOT NULL,
    "trim" character varying(100) NOT NULL,
    body_style character varying(50),
    trim_level smallint DEFAULT 1,
    engine_l numeric(3,1),
    engine_desc character varying(100),
    cylinders smallint,
    horsepower smallint,
    torque_lb_ft smallint,
    fuel_type character varying(30),
    forced_induction character varying(25) DEFAULT 'None'::character varying,
    transmission character varying(30),
    trans_speeds smallint,
    drivetrain character varying(10),
    mpg_city smallint,
    mpg_highway smallint,
    mpg_combined smallint,
    range_miles smallint,
    base_msrp integer,
    source character varying(20) DEFAULT 'epa'::character varying NOT NULL,
    notes text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    search_vector tsvector,
    embedding public.vector(1536),
    CONSTRAINT vehicles_year_check CHECK (((year >= 2000) AND (year <= 2030)))
);


--
-- Name: dealer_recipes; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.dealer_recipes (
    dealer_id text NOT NULL,
    recipes_json text NOT NULL,
    recipe_count integer DEFAULT 0 NOT NULL,
    provider_hint text,
    max_saved_at double precision DEFAULT 0 NOT NULL,
    last_ok_at double precision DEFAULT 0 NOT NULL,
    stale_count integer DEFAULT 0 NOT NULL,
    updated_at text,
    scan_hints text
);


--
-- Name: dealer_reviews; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.dealer_reviews (
    id bigint NOT NULL,
    dealer_id text NOT NULL,
    user_id integer NOT NULL,
    display_name text,
    is_anonymous integer DEFAULT 0 NOT NULL,
    rating integer NOT NULL,
    body text NOT NULL,
    addon_fee_reported integer DEFAULT 0 NOT NULL,
    addon_fee_amount double precision,
    addon_fee_desc text,
    status text DEFAULT 'published'::text NOT NULL,
    report_count integer DEFAULT 0 NOT NULL,
    ip_hash text,
    created_at text,
    updated_at text
);


--
-- Name: dealer_reviews_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.dealer_reviews_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: dealer_reviews_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.dealer_reviews_id_seq OWNED BY public.dealer_reviews.id;


--
-- Name: dealer_specials; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.dealer_specials (
    id bigint NOT NULL,
    dealer_id text NOT NULL,
    title text,
    type text,
    vehicle_year integer,
    vehicle_make text,
    vehicle_model text,
    vehicle_trim text,
    payment double precision,
    term_months integer,
    due_at_signing double precision,
    mileage_per_year integer,
    msrp double precision,
    expires text,
    fine_print text,
    source_url text,
    raw_html_snippet text,
    offer_hash text,
    scraped_at text NOT NULL
);


--
-- Name: dealer_specials_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.dealer_specials_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: dealer_specials_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.dealer_specials_id_seq OWNED BY public.dealer_specials.id;


--
-- Name: dictionary_options; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.dictionary_options (
    id bigint NOT NULL,
    year integer,
    make text,
    model text,
    "trim" text,
    engine_options text,
    engine_display text,
    forced_induction text,
    transmission text,
    drivetrain text,
    fuel_type text,
    body_style text,
    cylinders integer,
    displacement double precision,
    mpg_city double precision,
    mpg_highway double precision,
    mpg_combined double precision,
    exterior_colors text,
    packages text,
    package_details text,
    options text,
    option_details text
);


--
-- Name: dictionary_options_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.dictionary_options_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: dictionary_options_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.dictionary_options_id_seq OWNED BY public.dictionary_options.id;


--
-- Name: exterior_colors_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.exterior_colors_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: exterior_colors_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.exterior_colors_id_seq OWNED BY public.catalog_exterior_colors.id;


--
-- Name: interior_colors_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.interior_colors_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: interior_colors_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.interior_colors_id_seq OWNED BY public.catalog_interior_colors.id;


--
-- Name: lease_offer_matches; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.lease_offer_matches (
    id bigint NOT NULL,
    dealer_id text NOT NULL,
    offer_hash text NOT NULL,
    offer_id bigint,
    extracted_json text,
    summary text,
    matches_json text,
    match_count integer DEFAULT 0,
    confidence double precision,
    computed_at text NOT NULL
);


--
-- Name: lease_offer_matches_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.lease_offer_matches_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: lease_offer_matches_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.lease_offer_matches_id_seq OWNED BY public.lease_offer_matches.id;


--
-- Name: market_price_stats; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.market_price_stats (
    year integer NOT NULL,
    make text NOT NULL,
    model text NOT NULL,
    "trim" text NOT NULL,
    condition text NOT NULL,
    mileage_band_low integer NOT NULL,
    mileage_band_high integer,
    median_price double precision NOT NULL,
    p25_price double precision NOT NULL,
    p75_price double precision NOT NULL,
    sample_count integer NOT NULL,
    dealer_count integer NOT NULL,
    computed_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: model_generations; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.model_generations (
    id bigint NOT NULL,
    make text NOT NULL,
    model text NOT NULL,
    generation text NOT NULL,
    year_start integer NOT NULL,
    year_end integer,
    notes text,
    status text DEFAULT 'estimated'::text NOT NULL
);


--
-- Name: model_generations_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.model_generations_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: model_generations_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.model_generations_id_seq OWNED BY public.model_generations.id;


--
-- Name: package_features_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.package_features_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: package_features_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.package_features_id_seq OWNED BY public.catalog_package_features.id;


--
-- Name: package_values_msrp_quarantine; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.package_values_msrp_quarantine (
    id bigint NOT NULL,
    year integer,
    make text NOT NULL,
    model text NOT NULL,
    "trim" text NOT NULL,
    kind text NOT NULL,
    match_key text NOT NULL,
    name_display text NOT NULL,
    name_norm text NOT NULL,
    code text,
    msrp double precision,
    msrp_source text,
    msrp_authority integer NOT NULL,
    category text,
    observation_count integer NOT NULL,
    sticker_count integer NOT NULL,
    confidence double precision NOT NULL,
    first_seen_at text,
    last_seen_at text,
    quarantined_at timestamp with time zone DEFAULT now() NOT NULL,
    label text,
    reason text
);


--
-- Name: packages_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.packages_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: packages_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.packages_id_seq OWNED BY public.catalog_packages.id;


--
-- Name: standalone_options_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.standalone_options_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: standalone_options_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.standalone_options_id_seq OWNED BY public.catalog_options.id;


--
-- Name: vehicles_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.vehicles_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: vehicles_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.vehicles_id_seq OWNED BY public.catalog_trims.id;


--
-- Name: car_move_log id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.car_move_log ALTER COLUMN id SET DEFAULT nextval('public.car_move_log_id_seq'::regclass);


--
-- Name: catalog_exterior_colors id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.catalog_exterior_colors ALTER COLUMN id SET DEFAULT nextval('public.exterior_colors_id_seq'::regclass);


--
-- Name: catalog_interior_colors id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.catalog_interior_colors ALTER COLUMN id SET DEFAULT nextval('public.interior_colors_id_seq'::regclass);


--
-- Name: catalog_options id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.catalog_options ALTER COLUMN id SET DEFAULT nextval('public.standalone_options_id_seq'::regclass);


--
-- Name: catalog_package_features id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.catalog_package_features ALTER COLUMN id SET DEFAULT nextval('public.package_features_id_seq'::regclass);


--
-- Name: catalog_packages id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.catalog_packages ALTER COLUMN id SET DEFAULT nextval('public.packages_id_seq'::regclass);


--
-- Name: catalog_trims id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.catalog_trims ALTER COLUMN id SET DEFAULT nextval('public.vehicles_id_seq'::regclass);


--
-- Name: dealer_reviews id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dealer_reviews ALTER COLUMN id SET DEFAULT nextval('public.dealer_reviews_id_seq'::regclass);


--
-- Name: dealer_specials id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dealer_specials ALTER COLUMN id SET DEFAULT nextval('public.dealer_specials_id_seq'::regclass);


--
-- Name: dictionary_options id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dictionary_options ALTER COLUMN id SET DEFAULT nextval('public.dictionary_options_id_seq'::regclass);


--
-- Name: lease_offer_matches id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.lease_offer_matches ALTER COLUMN id SET DEFAULT nextval('public.lease_offer_matches_id_seq'::regclass);


--
-- Name: model_generations id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.model_generations ALTER COLUMN id SET DEFAULT nextval('public.model_generations_id_seq'::regclass);


--
-- Name: car_move_log car_move_log_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.car_move_log
    ADD CONSTRAINT car_move_log_pkey PRIMARY KEY (id);


--
-- Name: dealer_recipes dealer_recipes_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dealer_recipes
    ADD CONSTRAINT dealer_recipes_pkey PRIMARY KEY (dealer_id);


--
-- Name: dealer_reviews dealer_reviews_dealer_id_user_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dealer_reviews
    ADD CONSTRAINT dealer_reviews_dealer_id_user_id_key UNIQUE (dealer_id, user_id);


--
-- Name: dealer_reviews dealer_reviews_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dealer_reviews
    ADD CONSTRAINT dealer_reviews_pkey PRIMARY KEY (id);


--
-- Name: dealer_specials dealer_specials_dealer_id_offer_hash_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dealer_specials
    ADD CONSTRAINT dealer_specials_dealer_id_offer_hash_key UNIQUE (dealer_id, offer_hash);


--
-- Name: dealer_specials dealer_specials_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dealer_specials
    ADD CONSTRAINT dealer_specials_pkey PRIMARY KEY (id);


--
-- Name: dictionary_options dictionary_options_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dictionary_options
    ADD CONSTRAINT dictionary_options_pkey PRIMARY KEY (id);


--
-- Name: catalog_exterior_colors exterior_colors_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.catalog_exterior_colors
    ADD CONSTRAINT exterior_colors_pkey PRIMARY KEY (id);


--
-- Name: catalog_exterior_colors exterior_colors_vehicle_id_color_name_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.catalog_exterior_colors
    ADD CONSTRAINT exterior_colors_vehicle_id_color_name_key UNIQUE (vehicle_id, color_name);


--
-- Name: catalog_interior_colors interior_colors_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.catalog_interior_colors
    ADD CONSTRAINT interior_colors_pkey PRIMARY KEY (id);


--
-- Name: catalog_interior_colors interior_colors_vehicle_id_color_name_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.catalog_interior_colors
    ADD CONSTRAINT interior_colors_vehicle_id_color_name_key UNIQUE (vehicle_id, color_name);


--
-- Name: lease_offer_matches lease_offer_matches_dealer_id_offer_hash_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.lease_offer_matches
    ADD CONSTRAINT lease_offer_matches_dealer_id_offer_hash_key UNIQUE (dealer_id, offer_hash);


--
-- Name: lease_offer_matches lease_offer_matches_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.lease_offer_matches
    ADD CONSTRAINT lease_offer_matches_pkey PRIMARY KEY (id);


--
-- Name: market_price_stats market_price_stats_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.market_price_stats
    ADD CONSTRAINT market_price_stats_pkey PRIMARY KEY (make, model, "trim", year, condition, mileage_band_low);


--
-- Name: model_generations model_generations_make_model_generation_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.model_generations
    ADD CONSTRAINT model_generations_make_model_generation_key UNIQUE (make, model, generation);


--
-- Name: model_generations model_generations_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.model_generations
    ADD CONSTRAINT model_generations_pkey PRIMARY KEY (id);


--
-- Name: catalog_package_features package_features_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.catalog_package_features
    ADD CONSTRAINT package_features_pkey PRIMARY KEY (id);


--
-- Name: catalog_packages packages_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.catalog_packages
    ADD CONSTRAINT packages_pkey PRIMARY KEY (id);


--
-- Name: catalog_packages packages_vehicle_id_package_name_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.catalog_packages
    ADD CONSTRAINT packages_vehicle_id_package_name_key UNIQUE (vehicle_id, package_name);


--
-- Name: catalog_options standalone_options_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.catalog_options
    ADD CONSTRAINT standalone_options_pkey PRIMARY KEY (id);


--
-- Name: catalog_options standalone_options_vehicle_id_option_name_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.catalog_options
    ADD CONSTRAINT standalone_options_vehicle_id_option_name_key UNIQUE (vehicle_id, option_name);


--
-- Name: catalog_trims vehicles_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.catalog_trims
    ADD CONSTRAINT vehicles_pkey PRIMARY KEY (id);


--
-- Name: catalog_trims vehicles_year_make_model_trim_engine_drivetrain_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.catalog_trims
    ADD CONSTRAINT vehicles_year_make_model_trim_engine_drivetrain_key UNIQUE NULLS NOT DISTINCT (year, make, model, "trim", engine_desc, drivetrain);


--
-- Name: idx_car_move_log_car; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_car_move_log_car ON public.car_move_log USING btree (car_id, moved_at DESC);


--
-- Name: idx_dealer_reviews_dealer; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_dealer_reviews_dealer ON public.dealer_reviews USING btree (dealer_id, status);


--
-- Name: idx_dealer_reviews_user; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_dealer_reviews_user ON public.dealer_reviews USING btree (user_id, created_at);


--
-- Name: idx_dealer_specials_dealer; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_dealer_specials_dealer ON public.dealer_specials USING btree (dealer_id);


--
-- Name: idx_dict_options_lookup; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_dict_options_lookup ON public.dictionary_options USING btree (year, make, model);


--
-- Name: idx_dict_options_trim; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_dict_options_trim ON public.dictionary_options USING btree (year, make, model, "trim");


--
-- Name: idx_exterior_colors_vehicle; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_exterior_colors_vehicle ON public.catalog_exterior_colors USING btree (vehicle_id);


--
-- Name: idx_interior_colors_vehicle; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_interior_colors_vehicle ON public.catalog_interior_colors USING btree (vehicle_id);


--
-- Name: idx_lease_matches_dealer; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_lease_matches_dealer ON public.lease_offer_matches USING btree (dealer_id);


--
-- Name: idx_package_features_package; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_package_features_package ON public.catalog_package_features USING btree (package_id);


--
-- Name: idx_packages_vehicle; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_packages_vehicle ON public.catalog_packages USING btree (vehicle_id);


--
-- Name: idx_standalone_options_vehicle; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_standalone_options_vehicle ON public.catalog_options USING btree (vehicle_id);


--
-- Name: idx_vehicles_embedding; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_vehicles_embedding ON public.catalog_trims USING hnsw (embedding public.vector_cosine_ops);


--
-- Name: idx_vehicles_fuel_type; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_vehicles_fuel_type ON public.catalog_trims USING btree (fuel_type);


--
-- Name: idx_vehicles_make; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_vehicles_make ON public.catalog_trims USING btree (make);


--
-- Name: idx_vehicles_search; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_vehicles_search ON public.catalog_trims USING gin (search_vector);


--
-- Name: idx_vehicles_year_make_model; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_vehicles_year_make_model ON public.catalog_trims USING btree (year, make, model);


--
-- Name: ux_ai_engine_specs; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX ux_ai_engine_specs ON public.ai_engine_specs USING btree (year, lower(make), lower(model), lower(btrim(engine_description)), COALESCE(cylinders, '-1'::integer), lower(btrim(fuel_type)));


--
-- Name: ux_ai_model_specs_ymm; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX ux_ai_model_specs_ymm ON public.ai_model_specs USING btree (year, lower(make), lower(model));


--
-- Name: catalog_trims trg_vehicles_search_vector; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER trg_vehicles_search_vector BEFORE INSERT OR UPDATE ON public.catalog_trims FOR EACH ROW EXECUTE FUNCTION public.refresh_vehicle_search_vector();


--
-- Name: catalog_trims trg_vehicles_updated_at; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER trg_vehicles_updated_at BEFORE UPDATE ON public.catalog_trims FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();


--
-- Name: catalog_exterior_colors exterior_colors_vehicle_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.catalog_exterior_colors
    ADD CONSTRAINT exterior_colors_vehicle_id_fkey FOREIGN KEY (vehicle_id) REFERENCES public.catalog_trims(id) ON DELETE CASCADE;


--
-- Name: catalog_interior_colors interior_colors_vehicle_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.catalog_interior_colors
    ADD CONSTRAINT interior_colors_vehicle_id_fkey FOREIGN KEY (vehicle_id) REFERENCES public.catalog_trims(id) ON DELETE CASCADE;


--
-- Name: catalog_package_features package_features_package_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.catalog_package_features
    ADD CONSTRAINT package_features_package_id_fkey FOREIGN KEY (package_id) REFERENCES public.catalog_packages(id) ON DELETE CASCADE;


--
-- Name: catalog_packages packages_vehicle_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.catalog_packages
    ADD CONSTRAINT packages_vehicle_id_fkey FOREIGN KEY (vehicle_id) REFERENCES public.catalog_trims(id) ON DELETE CASCADE;


--
-- Name: catalog_options standalone_options_vehicle_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.catalog_options
    ADD CONSTRAINT standalone_options_vehicle_id_fkey FOREIGN KEY (vehicle_id) REFERENCES public.catalog_trims(id) ON DELETE CASCADE;


--
-- PostgreSQL database dump complete
--


