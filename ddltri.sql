create schema logddl;

create table logddl.ddl_log(id serial,
username text,
timestamp timestamptz default current_timestamp,
schema_name text,
object_name text,
ddl_type text,
ddl_query text) distributed by (id);

create function logddl.ddl_log_func()
returns event_trigger
language plpgsql
as 
  $$
    declare
      query text;
      command record;
    begin
      query := current_query();
      if exists(select * from pg_event_trigger_ddl_commands()) then
        for command in select * from pg_event_trigger_ddl_commands()
          loop
            insert into logddl.ddl_log(username, schema_name, object_name, ddl_type, ddl_query)
            values(current_user, command.schema_name, command.object_identity, command.command_tag, query);
          end loop;
      end if;
    end;
  $$
security definer;

create event trigger ddl_log_tri on ddl_command_end execute procedure logddl.ddl_log_func();
