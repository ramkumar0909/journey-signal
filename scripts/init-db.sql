-- Disposable local roles only. Runtime cannot modify the schema or create users.
CREATE ROLE journeysignal_demo LOGIN PASSWORD 'disposable_demo_only' NOSUPERUSER;
CREATE ROLE journeysignal_reader LOGIN PASSWORD 'disposable_reader_only' NOSUPERUSER;
GRANT CONNECT ON DATABASE journeysignal TO journeysignal_demo, journeysignal_reader;
GRANT USAGE ON SCHEMA public TO journeysignal_demo, journeysignal_reader;
ALTER DEFAULT PRIVILEGES FOR ROLE journeysignal_owner IN SCHEMA public
    GRANT SELECT, INSERT, UPDATE ON TABLES TO journeysignal_demo;
ALTER DEFAULT PRIVILEGES FOR ROLE journeysignal_owner IN SCHEMA public
    GRANT SELECT ON TABLES TO journeysignal_reader;
