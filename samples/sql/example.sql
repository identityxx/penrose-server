drop table categories;

create table categories (
    id int(11),
    name varchar(50),
    description text,
    primary key (id)
);

insert into categories values (1, 'Bakery', 'Bagels, Biscuits, Rolls, Bread, Buns, Muffins, Desserts, Tortillas');
insert into categories values (2, 'Beverages', 'Beer, Wine, Coffee, Cocoa, Tea, Soft Drinks, Water');
insert into categories values (3, 'Deli', 'Cheeses, Meats, Party Trays, Sides, Snacks');
insert into categories values (4, 'Grocery', 'Breakfast Foods, Canned Food, Ethnic Foods, Pasta, Rice, Soups');
insert into categories values (5, 'Meat & Poultry', 'Beef, Chicken, Frozen, Lamb, Pork, Turkey');
insert into categories values (6, 'Produce', 'Fruit, Vegetables');
insert into categories values (7, 'Seafood', 'Fish, Shellfish');

drop table products;

create table products (
    id int(11),
    categoryId int(11),
    name varchar(50),
    price double,
    primary key (id)
);

insert into products values (1, 1, 'Bagel', 3);
insert into products values (2, 1, 'Baguette', 2);
insert into products values (3, 1, 'French Bread', 3);
insert into products values (4, 1, 'Taco shell', 1);
insert into products values (5, 1, 'Tortilla', 4);
insert into products values (6, 2, 'Beer', 3);
insert into products values (7, 2, 'Champagne', 5);
insert into products values (8, 2, 'Wine', 6);
insert into products values (9, 2, 'Soda', 1);
insert into products values (10, 2, 'Root Beer', 1);
insert into products values (11, 3, 'Provolone', 3);
insert into products values (12, 3, 'Mozzarella', 4);
insert into products values (13, 3, 'Cheddar', 2);
insert into products values (14, 4, 'Cereal', 5);
insert into products values (15, 4, 'Lasagne', 2);
insert into products values (16, 4, 'Rice', 3);
insert into products values (17, 5, 'Ground Beef', 4);
insert into products values (18, 5, 'Ribeye', 5);
insert into products values (19, 5, 'Chicken', 3);
insert into products values (20, 5, 'Bacon', 3);
insert into products values (21, 5, 'Pork Chop', 5);
insert into products values (22, 6, 'Banana', 2);
insert into products values (23, 6, 'Grape', 2);
insert into products values (24, 6, 'Lemon', 1);
insert into products values (25, 6, 'Tomatoe', 1);
insert into products values (26, 6, 'Mushroom', 1);
insert into products values (27, 7, 'Catfish', 3);
insert into products values (28, 7, 'Salmon', 5);
insert into products values (29, 7, 'Crab', 7);
insert into products values (30, 7, 'Shrimp', 5);

drop table order_details;

create table order_details (
    orderId int(11),
    productId int(11),
    price double,
    quantity int(11),
    primary key (orderId, productId)
);

insert into order_details values (1, 8, 6, 2);
insert into order_details values (1, 16, 3, 1);
insert into order_details values (2, 2, 2, 1);
insert into order_details values (2, 26, 1, 2);
insert into order_details values (3, 17, 4, 1);
insert into order_details values (4, 14, 5, 3);
insert into order_details values (4, 30, 5, 2);
insert into order_details values (5, 9, 1, 1);
insert into order_details values (6, 30, 5, 2);
insert into order_details values (6, 8, 6, 1);
insert into order_details values (6, 15, 2, 1);
insert into order_details values (7, 2, 2, 2);
insert into order_details values (8, 8, 6, 1);
insert into order_details values (9, 16, 3, 3);
insert into order_details values (10, 24, 1, 1);

drop table orders;

create table orders (
    id int(11),
    customerId int(11),
    date datetime,
    primary key (id)
);

insert into orders values (1, 3, '2004-08-10');
insert into orders values (2, 2, '2004-12-22');
insert into orders values (3, 3, '2005-02-02');
insert into orders values (4, 2, '2005-02-10');
insert into orders values (5, 5, '2005-02-10');
insert into orders values (6, 3, '2005-04-17');
insert into orders values (7, 1, '2005-05-25');
insert into orders values (8, 1, '2005-07-11');
insert into orders values (9, 6, '2005-08-09');
insert into orders values (10, 4, '2005-09-07');

drop table customers;

create table customers (
    id int(11),
    firstName varchar(50),
    lastName varchar(50),
    primary key (id)
);

insert into customers values (1, 'Ted', 'Morris');
insert into customers values (2, 'Sam', 'Carter');
insert into customers values (3, 'Eric', 'Walker');
insert into customers values (4, 'Randy', 'Mills');
insert into customers values (5, 'Janet', 'Carter');
insert into customers values (6, 'Karen', 'Jensen');
insert into customers values (7, 'Martin', 'Hunter');
insert into customers values (8, 'Ted', 'Jensen');
insert into customers values (9, 'Eric', 'Morris');
insert into customers values (10, 'Benjamin', 'Hall');
