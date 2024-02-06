-- ������� order-purchase �� ����� �.�. ����� ����� order � purchase - ������ � ������
-- ������� ����������� not null � ��������� ����������
create table product(
productId int primary key identity(1,1),
productName varchar(255) not null,
price numeric(10, 2) not null
);
create table "user"(
userId int primary key identity(1,1),
fio varchar(255) not null,
mail varchar(255) not null
);
create table "order"(
num int primary key,
userId int references "user"(userId) not null,
regDate date not null,
"sum" int
);
create table purchase(
purchaseId int primary key identity(1,1),
productId int references product(productId) not null,
num int references "order"(num) not null,
quantity int not null
);
-- ��� ������ ��������� ������
select * from product;
select * from "user";
select * from purchase;
select * from "order";

--����� ����� ������� ����� ��������

drop table purchase;
drop table "order";
drop table product;
drop table "user";