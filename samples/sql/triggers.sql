-- categories --

drop table categories_changes;

create table categories_changes (
    changeNumber integer auto_increment,
    changeTime datetime,
    changeAction varchar(10),
    id integer,
    primary key (changeNumber)
);

drop trigger catagories_add;

create trigger catagories_add after insert on categories
for each row insert into categories_changes values (null, now(), 'ADD', new.id);

drop trigger catagories_modify;

delimiter |
create trigger catagories_modify after update on categories
for each row begin
    if new.id = old.id then
        insert into categories_changes values (null, now(), 'MODIFY', new.id);
    else
        insert into categories_changes values (null, now(), 'DELETE', old.id);
        insert into categories_changes values (null, now(), 'ADD', new.id);
    end if;
end;|
delimiter ;

drop trigger catagories_delete;

create trigger catagories_delete after delete on categories
for each row insert into categories_changes values (null, now(), 'DELETE', old.id);

-- customer_emails --

drop table customer_emails_changes;

create table customer_emails_changes (
    changeNumber integer auto_increment,
    changeTime datetime,
    changeAction varchar(10),
    email varchar(50),
    primary key (changeNumber)
);

drop trigger customer_emails_add;

create trigger customer_emails_add after insert on customer_emails
for each row insert into customer_emails_changes values (null, now(), 'ADD', new.email);

drop trigger customer_emails_modify;

delimiter |
create trigger customer_emails_modify after update on customer_emails
for each row begin
    if new.email = old.email then
        insert into customer_emails_changes values (null, now(), 'MODIFY', new.email);
    else
        insert into customer_emails_changes values (null, now(), 'DELETE', old.email);
        insert into customer_emails_changes values (null, now(), 'ADD', new.email);
    end if;
end;|
delimiter ;

drop trigger customer_emails_delete;

create trigger customer_emails_delete after delete on customer_emails
for each row insert into customer_emails_changes values (null, now(), 'DELETE', old.email);

-- customers --

drop table customers_changes;

create table customers_changes (
    changeNumber integer auto_increment,
    changeTime datetime,
    changeAction varchar(10),
    username varchar(10),
    primary key (changeNumber)
);

drop trigger customers_add;

create trigger customers_add after insert on customers
for each row insert into customers_changes values (null, now(), 'ADD', new.username);

drop trigger customers_modify;

delimiter |
create trigger customers_modify after update on customers
for each row begin
    if new.username = old.username then
        insert into customers_changes values (null, now(), 'MODIFY', new.username);
    else
        insert into customers_changes values (null, now(), 'DELETE', old.username);
        insert into customers_changes values (null, now(), 'ADD', new.username);
    end if;
end;|
delimiter ;

drop trigger customers_delete;

create trigger customers_delete after delete on customers
for each row insert into customers_changes values (null, now(), 'DELETE', old.username);

-- employees --

drop table employees_changes;

create table employees_changes (
    changeNumber integer auto_increment,
    changeTime datetime,
    changeAction varchar(10),
    username varchar(10),
    primary key (changeNumber)
);

drop trigger employees_add;

create trigger employees_add after insert on employees
for each row insert into employees_changes values (null, now(), 'ADD', new.username);

drop trigger employees_modify;

delimiter |
create trigger employees_modify after update on employees
for each row begin
    if new.username = old.username then
        insert into employees_changes values (null, now(), 'MODIFY', new.username);
    else
        insert into employees_changes values (null, now(), 'DELETE', old.username);
        insert into employees_changes values (null, now(), 'ADD', new.username);
    end if;
end;|
delimiter ;

drop trigger employees_delete;

create trigger employees_delete after delete on employees
for each row insert into employees_changes values (null, now(), 'DELETE', old.username);

-- order_details --

drop table order_details_changes;

create table order_details_changes (
    changeNumber integer auto_increment,
    changeTime datetime,
    changeAction varchar(10),
    orderId integer,
    productId integer,
    primary key (changeNumber)
);

drop trigger order_details_add;

create trigger order_details_add after insert on order_details
for each row insert into order_details_changes values (null, now(), 'ADD', new.orderId, new.productId);

drop trigger order_details_modify;

delimiter |
create trigger order_details_modify after update on order_details
for each row begin
    if new.orderId = old.orderId and new.productId = old.productId then
        insert into order_details_changes values (null, now(), 'MODIFY', new.orderId, new.productId);
    else
        insert into order_details_changes values (null, now(), 'DELETE', old.orderId, old.productId);
        insert into order_details_changes values (null, now(), 'ADD', new.orderId, new.productId);
    end if;
end;|
delimiter ;

drop trigger order_details_delete;

create trigger order_details_delete after delete on order_details
for each row insert into order_details_changes values (null, now(), 'DELETE', old.orderId, old.productId);

-- order_histories --

drop table order_histories_changes;

create table order_histories_changes (
    changeNumber integer auto_increment,
    changeTime datetime,
    changeAction varchar(10),
    orderId integer,
    productId integer,
    historyDate datetime,
    primary key (changeNumber)
);

drop trigger order_histories_add;

create trigger order_histories_add after insert on order_histories
for each row insert into order_histories_changes values (null, now(), 'ADD', new.orderId, new.productId, new.historyDate);

drop trigger order_histories_modify;

delimiter |
create trigger order_histories_modify after update on order_histories
for each row begin
    if new.orderId = old.orderId and new.productId = old.productId and new.historyDate = old.historyDate then
        insert into order_histories_changes values (null, now(), 'MODIFY', new.orderId, new.productId, new.historyDate);
    else
        insert into order_histories_changes values (null, now(), 'DELETE', old.orderId, old.productId, old.historyDate);
        insert into order_histories_changes values (null, now(), 'ADD', new.orderId, new.productId, new.historyDate);
    end if;
end;|
delimiter ;

drop trigger order_histories_delete;

create trigger order_histories_delete after delete on order_histories
for each row insert into order_histories_changes values (null, now(), 'DELETE', old.orderId, old.productId, old.historyDate);

-- orders --

drop table orders_changes;

create table orders_changes (
    changeNumber integer auto_increment,
    changeTime datetime,
    changeAction varchar(10),
    id integer,
    primary key (changeNumber)
);

drop trigger orders_add;

create trigger orders_add after insert on orders
for each row insert into orders_changes values (null, now(), 'ADD', new.id);

drop trigger orders_modify;

delimiter |
create trigger orders_modify after update on orders
for each row begin
    if new.id = old.id then
        insert into orders_changes values (null, now(), 'MODIFY', new.id);
    else
        insert into orders_changes values (null, now(), 'DELETE', old.id);
        insert into orders_changes values (null, now(), 'ADD', new.id);
    end if;
end;|
delimiter ;

drop trigger orders_delete;

create trigger orders_delete after delete on orders
for each row insert into orders_changes values (null, now(), 'DELETE', old.id);

-- products --

drop table products_changes;

create table products_changes (
    changeNumber integer auto_increment,
    changeTime datetime,
    changeAction varchar(10),
    id integer,
    primary key (changeNumber)
);

drop trigger products_add;

create trigger products_add after insert on products
for each row insert into products_changes values (null, now(), 'ADD', new.id);

drop trigger products_modify;

delimiter |
create trigger products_modify after update on products
for each row begin
    if new.id = old.id then
        insert into products_changes values (null, now(), 'MODIFY', new.id);
    else
        insert into products_changes values (null, now(), 'DELETE', old.id);
        insert into products_changes values (null, now(), 'ADD', new.id);
    end if;
end;|
delimiter ;

drop trigger products_delete;

create trigger products_delete after delete on products
for each row insert into products_changes values (null, now(), 'DELETE', old.id);
