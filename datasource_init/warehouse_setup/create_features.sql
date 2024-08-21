
-- Check current role
SELECT current_role;

-- Set to Super User
SET ROLE TO admin_user;

-- Create Schema
CREATE SCHEMA report AUTHORIZATION admin_user;
CREATE SCHEMA services AUTHORIZATION admin_user;
CREATE SCHEMA zones AUTHORIZATION admin_user;
CREATE SCHEMA trips AUTHORIZATION admin_user;

-- Check current schema
SHOW SEARCH_PATH;
