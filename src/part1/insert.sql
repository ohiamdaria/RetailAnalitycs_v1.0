INSERT INTO date_of_analysis_formation (analysis_formation)
VALUES (current_date);

INSERT INTO personal_data (customer_id, customer_name, customer_surname, customer_primary_email, customer_primary_phone)
VALUES (1,'Николай','Лаврентьев','714mexican732@mail.ru','+74911741649'),
       (2,'Ганс','Варенников','icbfb53hackeriigac@mail.ru','+74820550118'),
       (3,'Руслан','Лавров','i51tigeradda@mail.ru','+78519337901'),
       (4,'Руслан','Родионов','dbh7favoraihih@gmail.com','+74821326858'),
       (5,'Лев','Цирюльников','c9monk95@gmail.com','+74957781474');

INSERT INTO cards (customer_card_id, customer_id)
VALUES (1, 1),
       (2, 2),
       (3, 3),
       (4, 4),
       (5, 5);

INSERT INTO transactions (transaction_id, customer_card_id, transaction_summ, transaction_datetime, transaction_store_id)
VALUES (1,1,9860.098724, '08.12.2019 9:00:08', 18),
       (2,2,11475.05557,'02.12.2020 5:38:14', 17),
       (3,3,11894.08379,'12.10.2021 0:48:05',20),
    (4,4,264.199553,'20.04.2019 20:12:48',13),
    (5,5,2362.620701,'08.11.2019 12:35:55',9);

INSERT INTO groups_sku (group_id, group_name)
VALUES (1,'Чипсы'),
       (2,'Колбаса'),
       (3,'Ром'),
       (4,'Автомобили'),
       (5,'Зарядки');

INSERT INTO sku (sku_id, sku_name, group_id)
VALUES (1,'Heipz GmbH Масло подсолнечное Shadow',1),
       (2,'Heipz GmbH Анальгин Leta',2),
       (3,'ОАО РосМосГосТорг Ручка шариковая Red Line',3),
       (4, 'Heipz GmbH Апельсины Ревалон',4),
       (5, 'Kerton Квас Ревалон', 5);

INSERT INTO checks (transaction_id, sku_id, sku_amount, sku_summ, sku_summ_paid, sku_discount)
VALUES (1,1,1.126558383,56.41121885,54.32445678,2.086762072),
       (1,2,1.830726817, 827.9904571, 717.1919761, 110.798481),
       (1,3,6.622356954,4011.724194,3423.251255,588.4729389),
       (1,4,6.115034146,2303.07647,2303.07647,0),
       (1,5,1.794381942,32.11583428,32.11583428,0);


INSERT INTO stores (transaction_store_id, sku_id, sku_purchase_price, sku_retail_price)
VALUES (1,1, 88.7968052457072,137.77440630484),
       (1,2,615.685950625076,1016.08665857763),
       (1,3,132.441921346095, 199.208619201954),
       (1,4,77.3608189124432, 152.667755664182),
       (1,5,295.874959387758,360.772497152039);
