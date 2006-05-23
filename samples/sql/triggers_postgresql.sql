-- categories --

drop table categories_changes;

create table categories_changes (
    changeNumber serial,
    changeTime timestamp,
    changeAction varchar(10),
    changeUser varchar(10),
    id integer,
    primary key (changeNumber)
);

drop function catagories_add();

create or replace function categories_add() returns trigger as $$
begin
    insert into categories_changes values (default, now(), 'ADD', user, new.id);
    return null;
end;
$$ language plpgsql;

drop trigger catagories_add on categories;

create trigger catagories_add after insert on categories
for each row execute procedure categories_add();

drop function catagories_modify();

create or replace function categories_modify() returns trigger as $$
begin
    if new.id = old.id then
        insert into categories_changes values (default, now(), 'MODIFY', user, new.id);
    else
        insert into categories_changes values (default, now(), 'DELETE', user, old.id);
        insert into categories_changes values (default, now(), 'ADD', user, new.id);
    end if;
    return null;
end;
$$ language plpgsql;

drop trigger catagories_modify;

create trigger catagories_modify after update on categories
for each row execute procedure categories_modify();

drop function catagories_delete();

create or replace function categories_delete() returns trigger as $$
begin
    insert into categories_changes values (default, now(), 'DELETE', user, old.id);
    return null;
end;
$$ language plpgsql;

drop trigger catagories_delete;

create trigger catagories_delete after delete on categories
for each row execute procedure categories_delete();

-- customer_emails --

drop table customer_emails_changes;

create table customer_emails_changes (
    changeNumber serial,
    changeTime timestamp,
    changeAction varchar(10),
    changeUser varchar(10),
    email varchar(50),
    primary key (changeNumber)
);

drop function customer_emails_add();

create or replace function customer_emails_add() returns trigger as $$
begin
    insert into customer_emails_changes values (default, now(), 'ADD', user, new.email);
    return null;
end;
$$ language plpgsql;

drop trigger customer_emails_add;

create trigger customer_emails_add after insert on customer_emails
for each row execute procedure customer_emails_add();

drop function customer_emails_modify();

create or replace function customer_emails_modify() returns trigger as $$
begin
    if new.email = old.email then
        insert into customer_emails_changes values (default, now(), 'MODIFY', user, new.email);
    else
        insert into customer_emails_changes values (default, now(), 'DELETE', user, old.email);
        insert into customer_emails_changes values (default, now(), 'ADD', user, new.email);
    end if;
    return null;
end;
$$ language plpgsql;

drop trigger customer_emails_modify;

create trigger customer_emails_modify after update on customer_emails
for each row execute procedure customer_emails_modify();

drop function customer_emails_delete();

create or replace function customer_emails_delete() returns trigger as $$
begin
    insert into customer_emails_changes values (default, now(), 'DELETE', user, old.email);
    return null;
end;
$$ language plpgsql;

drop trigger customer_emails_delete;

create trigger customer_emails_delete after delete on customer_emails
for each row execute procedure customer_emails_delete();

-- customers --

drop table customers_changes;

create table customers_changes (
    changeNumber serial,
    changeTime timestamp,
    changeAction varchar(10),
    changeUser varchar(10),
    username varchar(10),
    primary key (changeNumber)
);

drop function customers_add();

create or replace function customers_add() returns trigger as $$
begin
    insert into customers_changes values (default, now(), 'ADD', user, new.username);
    return null;
end;
$$ language plpgsql;

drop trigger customers_add;

create trigger customers_add after insert on customers
for each row execute procedure customers_add();

drop function customers_modify();

create or replace function customers_modify() returns trigger as $$
begin
    if new.username = old.username then
        insert into customers_changes values (default, now(), 'MODIFY', user, new.username);
    else
        insert into customers_changes values (default, now(), 'DELETE', user, old.username);
        insert into customers_changes values (default, now(), 'ADD', user, new.username);
    end if;
    return null;
end;
$$ language plpgsql;

drop trigger customers_modify;

create trigger customers_modify after update on customers
for each row execute procedure customers_modify();

drop function customers_delete();

create or replace function customers_delete() returns trigger as $$
begin
    insert into customers_changes values (default, now(), 'DELETE', user, old.username);
    return null;
end;
$$ language plpgsql;

drop trigger customers_delete;

create trigger customers_delete after delete on customers
for each row execute procedure customers_delete();

-- employees --

drop table employees_changes;

create table employees_changes (
    changeNumber serial,
    changeTime timestamp,
    changeAction varchar(10),
    changeUser varchar(10),
    username varchar(10),
    primary key (changeNumber)
);

drop function employees_add();

create or replace function employees_add() returns trigger as $$
begin
    insert into employees_changes values (default, now(), 'ADD', user, new.username);
    return null;
end;
$$ language plpgsql;

drop trigger employees_add;

create trigger employees_add after insert on employees
for each row execute procedure employees_add();

drop function employees_modify();

create or replace function employees_modify() returns trigger as $$
begin
    if new.username = old.username then
        insert into employees_changes values (default, now(), 'MODIFY', user, new.username);
    else
        insert into employees_changes values (default, now(), 'DELETE', user, old.username);
        insert into employees_changes values (default, now(), 'ADD', user, new.username);
    end if;
    return null;
end;
$$ language plpgsql;

drop trigger employees_modify;

create trigger employees_modify after update on employees
for each row execute procedure employees_modify();

drop function employees_delete();

create or replace function employees_delete() returns trigger as $$
begin
    insert into employees_changes values (default, now(), 'DELETE', user, old.username);
    return null;
end;
$$ language plpgsql;

drop trigger employees_delete;

create trigger employees_delete after delete on employees
for each row execute procedure employees_delete();

-- order_details --

drop table order_details_changes;

create table order_details_changes (
    changeNumber serial,
    changeTime timestamp,
    changeAction varchar(10),
    changeUser varchar(10),
    orderId integer,
    productId integer,
    primary key (changeNumber)
);

drop function order_details_add();

create or replace function order_details_add() returns trigger as $$
begin
    insert into order_details_changes values (default, now(), 'ADD', user, new.orderId, new.productId);
    return null;
end;
$$ language plpgsql;

drop trigger order_details_add;

create trigger order_details_add after insert on order_details
for each row execute procedure order_details_add();

drop function order_details_modify();

create or replace function order_details_modify() returns trigger as $$
begin
    if new.orderId = old.orderId and new.productId = old.productId then
        insert into order_details_changes values (default, now(), 'MODIFY', user, new.orderId, new.productId);
    else
        insert into order_details_changes values (default, now(), 'DELETE', user, old.orderId, old.productId);
        insert into order_details_changes values (default, now(), 'ADD', user, new.orderId, new.productId);
    end if;
    return null;
end;
$$ language plpgsql;

drop trigger order_details_modify;

create trigger order_details_modify after update on order_details
for each row execute procedure order_details_modify();

drop function order_details_delete();

create or replace function order_details_delete() returns trigger as $$
begin
    insert into order_details_changes values (default, now(), 'DELETE', user, old.orderId, old.productId);
    return null;
end;
$$ language plpgsql;

drop trigger order_details_delete;

create trigger order_details_delete after delete on order_details
for each row execute procedure order_details_delete();

-- order_histories --

drop table order_histories_changes;

create table order_histories_changes (
    changeNumber serial,
    changeTime timestamp,
    changeAction varchar(10),
    changeUser varchar(10),
    orderId integer,
    productId integer,
    historyDate datetime,
    primary key (changeNumber)
);

drop function order_histories_add();

create or replace function order_histories_add() returns trigger as $$
begin
    insert into order_histories_changes values (default, now(), 'ADD', user, new.orderId, new.productId, new.historyDate);
    return null;
end;
$$ language plpgsql;

drop trigger order_histories_add;

create trigger order_histories_add after insert on order_histories
for each row execute procedure order_histories_add();

drop function order_histories_modify();

create or replace function order_histories_modify() returns trigger as $$
begin
    if new.orderId = old.orderId and new.productId = old.productId and new.historyDate = old.historyDate then
        insert into order_histories_changes values (default, now(), 'MODIFY', user, new.orderId, new.productId, new.historyDate);
    else
        insert into order_histories_changes values (default, now(), 'DELETE', user, old.orderId, old.productId, old.historyDate);
        insert into order_histories_changes values (default, now(), 'ADD', user, new.orderId, new.productId, new.historyDate);
    end if;
    return null;
end;
$$ language plpgsql;

drop trigger order_histories_modify;

create trigger order_histories_modify after update on order_histories
for each row execute procedure order_histories_modify();

drop function order_histories_delete();

create or replace function order_histories_delete() returns trigger as $$
begin
    insert into order_histories_changes values (default, now(), 'DELETE', user, old.orderId, old.productId, old.historyDate);
    return null;
end;
$$ language plpgsql;

drop trigger order_histories_delete;

create trigger order_histories_delete after delete on order_histories
for each row execute procedure order_histories_delete();

-- orders --

drop table orders_changes;

create table orders_changes (
    changeNumber serial,
    changeTime timestamp,
    changeAction varchar(10),
    changeUser varchar(10),
    id integer,
    primary key (changeNumber)
);

drop function orders_add();

create or replace function orders_add() returns trigger as $$
begin
    insert into orders_changes values (default, now(), 'ADD', user, new.id);
    return null;
end;
$$ language plpgsql;

drop trigger orders_add;

create trigger orders_add after insert on orders
for each row execute procedure orders_add();

drop function orders_modify();

create or replace function orders_modify() returns trigger as $$
begin
    if new.id = old.id then
        insert into orders_changes values (default, now(), 'MODIFY', user, new.id);
    else
        insert into orders_changes values (default, now(), 'DELETE', user, old.id);
        insert into orders_changes values (default, now(), 'ADD', user, new.id);
    end if;
    return null;
end;
$$ language plpgsql;

drop trigger orders_modify;

create trigger orders_modify after update on orders
for each row execute procedure orders_modify();

drop function orders_delete();

create or replace function orders_delete() returns trigger as $$
begin
    insert into orders_changes values (default, now(), 'DELETE', user, old.id);
    return null;
end;
$$ language plpgsql;

drop trigger orders_delete;

create trigger orders_delete after delete on orders
for each row execute procedure orders_delete();

-- products --

drop table products_changes;

create table products_changes (
    changeNumber serial,
    changeTime timestamp,
    changeAction varchar(10),
    changeUser varchar(10),
    id integer,
    primary key (changeNumber)
);

drop function products_add();

create or replace function products_add() returns trigger as $$
begin
    insert into products_changes values (default, now(), 'ADD', user, new.id);
    return null;
end;
$$ language plpgsql;

drop trigger products_add;

create trigger products_add after insert on products
for each row execute procedure products_add();

drop function products_modify();

create or replace function products_modify() returns trigger as $$
begin
    if new.id = old.id then
        insert into products_changes values (default, now(), 'MODIFY', user, new.id);
    else
        insert into products_changes values (default, now(), 'DELETE', user, old.id);
        insert into products_changes values (default, now(), 'ADD', user, new.id);
    end if;
    return null;
end;
$$ language plpgsql;

drop trigger products_modify;

create trigger products_modify after update on products
for each row execute procedure products_modify();

drop function products_delete();

create or replace function products_delete() returns trigger as $$
begin
    insert into products_changes values (default, now(), 'DELETE', user, old.id);
    return null;
end;
$$ language plpgsql;

drop trigger products_delete;

create trigger products_delete after delete on products
for each row execute procedure products_delete();
