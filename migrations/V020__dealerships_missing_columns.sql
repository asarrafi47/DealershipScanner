-- V020: dealerships gained these columns in local dev without a tracked migration.
ALTER TABLE public.dealerships ADD COLUMN IF NOT EXISTS oem_brand text;
ALTER TABLE public.dealerships ADD COLUMN IF NOT EXISTS business_status text;
ALTER TABLE public.dealerships ADD COLUMN IF NOT EXISTS google_primary_type text;
ALTER TABLE public.dealerships ADD COLUMN IF NOT EXISTS phone text;
ALTER TABLE public.dealerships ADD COLUMN IF NOT EXISTS platform text;
ALTER TABLE public.dealerships ADD COLUMN IF NOT EXISTS strategy text;
