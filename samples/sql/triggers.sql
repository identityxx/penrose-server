-- categories --

drop table categories_changes;

create table categories_changes (
    changeTime datetime,
    id integer,
    action varchar(10),
    primary key (changeTime, id)
);

drop trigger catagories_add;

create trigger catagories_add after insert on categories
for each row insert into categories_changes values (now(), new.id, 'ADD');

drop trigger catagories_modify;

delimiter |
create trigger catagories_modify after update on categories
for each row begin
    if new.id = old.id then
        insert into categories_changes values (now(), new.id, 'MODIFY');
    else
        insert into categories_changes values (now(), old.id, 'DELETE');
        insert into categories_changes values (now(), new.id, 'ADD');
    end if;
end;|
delimiter ;

drop trigger catagories_delete;

create trigger catagories_delete after delete on categories
for each row insert into categories_changes values (now(), old.id, 'DELETE');

-- customer_emails --

drop table customer_emails_changes;

create table customer_emails_changes (
    changeTime datetime,
    email varchar(50),
    action varchar(10),
    primary key (changeTime, email)
);

drop trigger customer_emails_add;

create trigger customer_emails_add after insert on customer_emails
for each row insert into customer_emails_changes values (now(), new.email, 'ADD');

drop trigger customer_emails_modify;

delimiter |
create trigger customer_emails_modify after update on customer_emails
for each row begin
    if new.email = old.email then
        insert into customer_emails_changes values (now(), new.email, 'MODIFY');
    else
        insert into customer_emails_changes values (now(), old.email, 'DELETE');
        insert into customer_emails_changes values (now(), new.email, 'ADD');
    end if;
end;|
delimiter ;

drop trigger customer_emails_delete;

create trigger customer_emails_delete after delete on customer_emails
for each row insert into customer_emails_changes values (now(), old.email, 'DELETE');

-- customers --

drop table customers_changes;

create table customers_changes (
    changeTime datetime,
    username varchar(10),
    action varchar(10),
    primary key (changeTime, username)
);

drop trigger customers_add;

create trigger customers_add after insert on customers
for each row insert into customers_changes values (now(), new.username, 'ADD');

drop trigger customers_modify;

delimiter |
create trigger customers_modify after update on customers
for each row begin
    if new.username = old.username then
        insert into customers_changes values (now(), new.username, 'MODIFY');
    else
        insert into customers_changes values (now(), old.username, 'DELETE');
        insert into customers_changes values (now(), new.username, 'ADD');
    end if;
end;|
delimiter ;

drop trigger customers_delete;

create trigger customers_delete after delete on customers
for each row insert into customers_changes values (now(), old.username, 'DELETE');

-- order_details --

drop table order_details_changes;

create table order_details_changes (
    changeTime datetime,
    orderId integer,
    productId integer,
    action varchar(10),
    primary key (changeTime, orderId, productId)
);

drop trigger order_details_add;

create trigger order_details_add after insert on order_details
for each row insert into order_details_changes values (now(), new.orderId, new.productId, 'ADD');

drop trigger order_details_modify;

delimiter |
create trigger order_details_modify after update on order_details
for each row begin
    if new.orderId = old.orderId and new.productId = old.productId then
        insert into order_details_changes values (now(), new.orderId, new.productId, 'MODIFY');
    else
        insert into order_details_changes values (now(), old.orderId, old.productId, 'DELETE');
        insert into order_details_changes values (now(), new.orderId, new.productId, 'ADD');
    end if;
end;|
delimiter ;

drop trigger order_details_delete;

create trigger order_details_delete after delete on order_details
for each row insert into order_details_changes values (now(), old.orderId, old.productId, 'DELETE');

-- orders --

drop table orders_changes;

create table orders_changes (
    changeTime datetime,
    id integer,
    action varchar(10),
    primary key (changeTime, id)
);

drop trigger orders_add;

create trigger orders_add after insert on orders
for each row insert into orders_changes values (now(), new.id, 'ADD');

drop trigger orders_modify;

delimiter |
create trigger orders_modify after update on orders
for each row begin
    if new.id = old.id then
        insert into orders_changes values (now(), new.id, 'MODIFY');
    else
        insert into orders_changes values (now(), old.id, 'DELETE');
        insert into orders_changes values (now(), new.id, 'ADD');
    end if;
end;|
delimiter ;

drop trigger orders_delete;

create trigger orders_delete after delete on orders
for each row insert into orders_changes values (now(), old.id, 'DELETE');

-- products --

drop table products_changes;

create table products_changes (
    changeTime datetime,
    id integer,
    action varchar(10),
    primary key (changeTime, id)
);

drop trigger products_add;

create trigger products_add after insert on products
for each row insert into products_changes values (now(), new.id, 'ADD');

drop trigger products_modify;

delimiter |
create trigger products_modify after update on products
for each row begin
    if new.id = old.id then
        insert into products_changes values (now(), new.id, 'MODIFY');
    else
        insert into products_changes values (now(), old.id, 'DELETE');
        insert into products_changes values (now(), new.id, 'ADD');
    end if;
end;|
delimiter ;

drop trigger products_delete;

create trigger products_delete after delete on products
for each row insert into products_changes values (now(), old.id, 'DELETE');
