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
insert into order_details values (11, 1, 6, 2);
insert into order_details values (11, 3, 3, 1);
insert into order_details values (12, 14, 2, 1);
insert into order_details values (12, 11, 1, 2);
insert into order_details values (13, 6, 4, 1);
insert into order_details values (14, 14, 5, 3);
insert into order_details values (14, 4, 5, 2);
insert into order_details values (15, 18, 1, 1);
insert into order_details values (16, 19, 5, 2);
insert into order_details values (16, 21, 6, 1);
insert into order_details values (16, 5, 2, 1);
insert into order_details values (17, 22, 2, 2);
insert into order_details values (18, 8, 6, 1);
insert into order_details values (19, 16, 3, 3);
insert into order_details values (20, 28, 1, 1);

drop table orders;

create table orders (
    id int(11),
    username varchar(10),
    date datetime,
    primary key (id)
);

insert into orders values (1, 'ewalker', '2004-08-10');
insert into orders values (2, 'scarter', '2004-12-22');
insert into orders values (3, 'ewalker', '2005-01-02');
insert into orders values (4, 'scarter', '2005-01-10');
insert into orders values (5, 'jcarter', '2005-02-10');
insert into orders values (6, 'ewalker', '2005-02-17');
insert into orders values (7, 'tmorris', '2005-02-25');
insert into orders values (8, 'tmorris', '2005-03-11');
insert into orders values (9, 'kjensen', '2005-04-09');
insert into orders values (10, 'jcarter', '2005-05-07');
insert into orders values (11, 'tmorris', '2005-05-15');
insert into orders values (12, 'ewalker', '2005-06-06');
insert into orders values (13, 'rmills', '2005-06-27');
insert into orders values (14, 'mhunter', '2005-07-14');
insert into orders values (15, 'rmills', '2005-07-14');
insert into orders values (16, 'jcarter', '2005-07-25');
insert into orders values (17, 'bhall', '2005-08-06');
insert into orders values (18, 'tmorris', '2005-08-18');
insert into orders values (19, 'scarter', '2005-09-07');
insert into orders values (20, 'emorris', '2005-09-12');

drop table customers;

create table customers (
    username varchar(10),
    firstName varchar(50),
    lastName varchar(50),
    password varchar(10),
    primary key (username)
);

insert into customers values ('tmorris', 'Ted', 'Morris', 'tm0rr1s');
insert into customers values ('scarter', 'Sam', 'Carter', '5c4rt3r');
insert into customers values ('ewalker', 'Eric', 'Walker', '3w4lk3r');
insert into customers values ('rmills', 'Randy', 'Mills', 'rm1ll5');
insert into customers values ('jcarter', 'Janet', 'Carter', 'jc4rt3r');
insert into customers values ('kjensen', 'Karen', 'Jensen', 'kj3n53n');
insert into customers values ('mhunter', 'Martin', 'Hunter', 'mhunt3r');
insert into customers values ('tjensen', 'Ted', 'Jensen', 'tj3n53n');
insert into customers values ('emorris', 'Eric', 'Morris', '3m0rr15');
insert into customers values ('bhall', 'Benjamin', 'Hall', 'bh4ll');
