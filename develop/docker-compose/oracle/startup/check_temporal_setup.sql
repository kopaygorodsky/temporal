-- This script runs at each container startup
-- It checks if the C##temporal user exists and has the correct privileges

-- Connect as SYSDBA (the script runs with SYSDBA privileges by default)
SET SERVEROUTPUT ON;

DECLARE
  user_exists NUMBER;
BEGIN
-- Check if the C##temporal user exists
SELECT COUNT(*) INTO user_exists FROM dba_users WHERE username = 'C##TEMPORAL';

IF user_exists = 0 THEN
    -- If user doesn't exist, create it and grant privileges
    DBMS_OUTPUT.PUT_LINE('Creating C##temporal user...');
EXECUTE IMMEDIATE 'CREATE USER C##temporal IDENTIFIED BY temporal CONTAINER=ALL';
EXECUTE IMMEDIATE 'GRANT CONNECT, RESOURCE TO C##temporal';
EXECUTE IMMEDIATE 'GRANT CREATE SESSION, CREATE TABLE, CREATE VIEW, CREATE SEQUENCE, CREATE TRIGGER, CREATE PROCEDURE TO C##temporal';
EXECUTE IMMEDIATE 'GRANT UNLIMITED TABLESPACE TO C##temporal';
ELSE
    DBMS_OUTPUT.PUT_LINE('C##temporal user already exists.');
END IF;

  -- Any other startup checks can go here
END;
/

EXIT SUCCESS;