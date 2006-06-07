-- categories --

drop sequence categories_changes_seq;

create sequence categories_changes_seq;

drop table categories_changes;

create table categories_changes (
    changeNumber integer,
    changeTime timestamp,
    changeAction varchar(10),
    changeUser varchar(10),
    id integer,
    primary key (changeNumber)
);

drop trigger catagories_add on categories;

create or replace trigger catagories_add after insert on categories
for each row begin
    insert into categories_changes values (categories_changes_seq.nextval, current_timestamp, 'ADD', user, :new.id);
end;
/

drop trigger catagories_modify;

create or replace trigger catagories_modify after update on categories
for each row begin
    if :new.id = :old.id then
        insert into categories_changes values (categories_changes_seq.nextval, current_timestamp, 'MODIFY', user, :new.id);
    else
        insert into categories_changes values (categories_changes_seq.nextval, current_timestamp, 'DELETE', user, :old.id);
        insert into categories_changes values (categories_changes_seq.nextval, current_timestamp, 'ADD', user, :new.id);
    end if;
end;
/

drop trigger catagories_delete;

create or replace trigger catagories_delete after delete on categories
for each row begin
    insert into categories_changes values (categories_changes_seq.nextval, current_timestamp, 'DELETE', user, :old.id);
end;
/

-- customer_emails --

drop sequence customer_emails_changes_seq;

create sequence customer_emails_changes_seq;

drop table customer_emails_changes;

create table customer_emails_changes (
    changeNumber integer,
    changeTime timestamp,
    changeAction varchar(10),
    changeUser varchar(10),
    email varchar(50),
    primary key (changeNumber)
);

drop trigger customer_emails_add;

create or replace trigger customer_emails_add after insert on customer_emails
for each row begin
    insert into customer_emails_changes values (customer_emails_changes_seq.nextval, current_timestamp, 'ADD', user, :new.email);
end;
/

drop trigger customer_emails_modify;

create or replace trigger customer_emails_modify after update on customer_emails
for each row begin
    if :new.email = :old.email then
        insert into customer_emails_changes values (customer_emails_changes_seq.nextval, current_timestamp, 'MODIFY', user, :new.email);
    else
        insert into customer_emails_changes values (customer_emails_changes_seq.nextval, current_timestamp, 'DELETE', user, :old.email);
        insert into customer_emails_changes values (customer_emails_changes_seq.nextval, current_timestamp, 'ADD', user, :new.email);
    end if;
end;
/

drop trigger customer_emails_delete;

create or replace trigger customer_emails_delete after delete on customer_emails
for each row begin
    insert into customer_emails_changes values (customer_emails_changes_seq.nextval, current_timestamp, 'DELETE', user, :old.email);
end;
/

-- customers --

drop sequence customers_changes_seq;

create sequence customers_changes_seq;

drop table customers_changes;

create table customers_changes (
    changeNumber integer,
    changeTime timestamp,
    changeAction varchar(10),
    changeUser varchar(10),
    username varchar(10),
    primary key (changeNumber)
);

drop trigger customers_add;

create or replace trigger customers_add after insert on customers
for each row begin
    insert into customers_changes values (customers_changes_seq.nextval, current_timestamp, 'ADD', user, :new.username);
end;
/

drop trigger customers_modify;

create or replace trigger customers_modify after update on customers
for each row begin
    if :new.username = :old.username then
        insert into customers_changes values (customers_changes_seq.nextval, current_timestamp, 'MODIFY', user, :new.username);
    else
        insert into customers_changes values (customers_changes_seq.nextval, current_timestamp, 'DELETE', user, :old.username);
        insert into customers_changes values (customers_changes_seq.nextval, current_timestamp, 'ADD', user, :new.username);
    end if;
end;
/

drop trigger customers_delete;

create or replace trigger customers_delete after delete on customers
for each row begin
    insert into customers_changes values (customers_changes_seq.nextval, current_timestamp, 'DELETE', user, :old.username);
end;
/

-- employees --

drop sequence employees_changes_seq;

create sequence employees_changes_seq;

drop table employees_changes;

create table employees_changes (
    changeNumber integer,
    changeTime timestamp,
    changeAction varchar(10),
    changeUser varchar(10),
    username varchar(10),
    primary key (changeNumber)
);

drop trigger employees_add;

create or replace trigger employees_add after insert on employees
for each row begin
    insert into employees_changes values (employees_changes_seq.nextval, current_timestamp, 'ADD', user, :new.username);
end;
/

drop trigger employees_modify;

create or replace trigger employees_modify after update on employees
for each row begin
    if :new.username = :old.username then
        insert into employees_changes values (employees_changes_seq.nextval, current_timestamp, 'MODIFY', user, :new.username);
    else
        insert into employees_changes values (employees_changes_seq.nextval, current_timestamp, 'DELETE', user, :old.username);
        insert into employees_changes values (employees_changes_seq.nextval, current_timestamp, 'ADD', user, :new.username);
    end if;
end;
/

drop trigger employees_delete;

create or replace trigger employees_delete after delete on employees
for each row begin
    insert into employees_changes values (employees_changes_seq.nextval, current_timestamp, 'DELETE', user, :old.username);
end;
/

-- order_details --

drop sequence order_details_changes_seq;

create sequence order_details_changes_seq;

drop table order_details_changes;

create table order_details_changes (
    changeNumber integer,
    changeTime timestamp,
    changeAction varchar(10),
    changeUser varchar(10),
    orderId integer,
    productId integer,
    primary key (changeNumber)
);

drop trigger order_details_add;

create or replace trigger order_details_add after insert on order_details
for each row begin
    insert into order_details_changes values (order_details_changes_seq.nextval, current_timestamp, 'ADD', user, :new.orderId, :new.productId);
end;
/

drop trigger order_details_modify;

create or replace trigger order_details_modify after update on order_details
for each row begin
    if :new.orderId = :old.orderId and :new.productId = :old.productId then
        insert into order_details_changes values (order_details_changes_seq.nextval, current_timestamp, 'MODIFY', user, :new.orderId, :new.productId);
    else
        insert into order_details_changes values (order_details_changes_seq.nextval, current_timestamp, 'DELETE', user, :old.orderId, :old.productId);
        insert into order_details_changes values (order_details_changes_seq.nextval, current_timestamp, 'ADD', user, :new.orderId, :new.productId);
    end if;
end;
/

drop trigger order_details_delete;

create or replace trigger order_details_delete after delete on order_details
for each row begin
    insert into order_details_changes values (order_details_changes_seq.nextval, current_timestamp, 'DELETE', user, :old.orderId, :old.productId);
end;
/

-- order_histories --

drop sequence order_histories_changes_seq;

create sequence order_histories_changes_seq;

drop table order_histories_changes;

create table order_histories_changes (
    changeNumber integer,
    changeTime timestamp,
    changeAction varchar(10),
    changeUser varchar(10),
    orderId integer,
    productId integer,
    historyDate timestamp,
    primary key (changeNumber)
);

drop trigger order_histories_add;

create or replace trigger order_histories_add after insert on order_histories
for each row begin
    insert into order_histories_changes values (order_histories_changes_seq.nextval, current_timestamp, 'ADD', user, :new.orderId, :new.productId, :new.historyDate);
end;
/

drop trigger order_histories_modify;

create or replace trigger order_histories_modify after update on order_histories
for each row begin
    if :new.orderId = :old.orderId and :new.productId = :old.productId and :new.historyDate = :old.historyDate then
        insert into order_histories_changes values (order_histories_changes_seq.nextval, current_timestamp, 'MODIFY', user, :new.orderId, :new.productId, :new.historyDate);
    else
        insert into order_histories_changes values (order_histories_changes_seq.nextval, current_timestamp, 'DELETE', user, :old.orderId, :old.productId, :old.historyDate);
        insert into order_histories_changes values (order_histories_changes_seq.nextval, current_timestamp, 'ADD', user, :new.orderId, :new.productId, :new.historyDate);
    end if;
end;
/

drop trigger order_histories_delete;

create or replace trigger order_histories_delete after delete on order_histories
for each row begin
    insert into order_histories_changes values (order_histories_changes_seq.nextval, current_timestamp, 'DELETE', user, :old.orderId, :old.productId, :old.historyDate);
end;
/

-- orders --

drop sequence orders_changes_seq;

create sequence orders_changes_seq;

drop table orders_changes;

create table orders_changes (
    changeNumber integer,
    changeTime timestamp,
    changeAction varchar(10),
    changeUser varchar(10),
    id integer,
    primary key (changeNumber)
);

drop trigger orders_add;

create or replace trigger orders_add after insert on orders
for each row begin
    insert into orders_changes values (orders_changes_seq.nextval, current_timestamp, 'ADD', user, :new.id);
end;
/

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

create or replace trigger orders_modify after update on orders
for each row begin
    if :new.id = :old.id then
        insert into orders_changes values (orders_changes_seq.nextval, current_timestamp, 'MODIFY', user, :new.id);
    else
        insert into orders_changes values (orders_changes_seq.nextval, current_timestamp, 'DELETE', user, :old.id);
        insert into orders_changes values (orders_changes_seq.nextval, current_timestamp, 'ADD', user, :new.id);
    end if;
end;
/

drop trigger orders_delete;

create or replace trigger orders_delete after delete on orders
for each row begin
    insert into orders_changes values (orders_changes_seq.nextval, current_timestamp, 'DELETE', user, :old.id);
end;
/

-- products --

drop sequence products_changes_seq;

create sequence products_changes_seq;

drop table products_changes;

create table products_changes (
    changeNumber integer,
    changeTime timestamp,
    changeAction varchar(10),
    changeUser varchar(10),
    id integer,
    primary key (changeNumber)
);

drop trigger products_add;

create or replace trigger products_add after insert on products
for each row begin
    insert into products_changes values (products_changes_seq.nextval, current_timestamp, 'ADD', user, :new.id);
end;
/

drop trigger products_modify;

create or replace trigger products_modify after update on products
for each row begin
    if :new.id = :old.id then
        insert into products_changes values (products_changes_seq.nextval, current_timestamp, 'MODIFY', user, :new.id);
    else
        insert into products_changes values (products_changes_seq.nextval, current_timestamp, 'DELETE', user, :old.id);
        insert into products_changes values (products_changes_seq.nextval, current_timestamp, 'ADD', user, :new.id);
    end if;
end;
/

drop trigger products_delete;

create or replace trigger products_delete after delete on products
for each row begin
    insert into products_changes values (products_changes_seq.nextval, current_timestamp, 'DELETE', user, :old.id);
end;
/
