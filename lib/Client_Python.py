import psycopg2

conn = psycopg2.connect(database='client_python', user='postgres')
with conn.cursor() as cur:
    cur.execute("""
        DROP TABLE customers;
        """)

def create_table(conn):
    """
    Функция, создающая структуру БД (таблицы)
    """
    with conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS customers(
        id SERIAL PRIMARY KEY,
        first_name VARCHAR(50),
        last_name VARCHAR(50),
        email VARCHAR(50),
        phones VARCHAR[],
        created_at TIMESTAMP DEFAULT NOW()
        );
        """)
        conn.commit()

def add_client(conn, first_name, last_name, email, phones=None):
    """
    добавить нового клиента.
    """
    with conn.cursor() as cur:
        cur.execute("""
        INSERT INTO customers(first_name, last_name, email, phones)
        VALUES(%s,%s,%s,%s);
        """, (first_name, last_name, email, phones))
        conn.commit()

def add_phones(conn, id, new_phones):
    """
    добавить телефон для существующего клиента.
    """
    with conn.cursor() as cur:
        cur.execute("""
        UPDATE customers SET phones=array_cat(phones, %s) WHERE id=%s;
        """, (new_phones, id))
        conn.commit()


def change_client(conn, id, first_name=None, last_name=None, email=None, phones=None):
    """
    изменить данные о клиенте.
    """
    with conn.cursor() as cur:
        add = "UPDATE customers SET"
        params = []

        if first_name is not None:
            add += ' first_name = %s,'
            params.append(first_name)
        if last_name is not None:
            add += ' last_name = %s,'
            params.append(last_name)
        if email is not None:
            add += ' email = %s,'
            params.append(email)
        if phones is not None:
            add += ' phones = %s,'
            params.append(phones)
        add = add.rstrip(',') + ' WHERE id = %s;'
        params.append(id)
        cur.execute(add, params)
        conn.commit()

def del_phone(conn, id, phone):
    """
    удалить телефон для существующего клиента.
    """
    with conn.cursor() as cur:
        cur.execute("""
        SELECT phones FROM customers WHERE id=%s;
        """, (id,))
        current_phones = cur.fetchone()[0]

        if current_phones is not None:
            current_phones = current_phones or []

            if phone in current_phones:
                current_phones.remove(phone)

                cur.execute("""
                UPDATE customers SET phones=%s WHERE id=%s;
                """, (current_phones,id))
                conn.commit()
            else:
                print(f'номер телефона {phone} в базе не найден')
        else:
            print('У клиента нет номеров')


def del_client(conn, first_name=None, last_name=None, email=None, phone=None):
    with conn.cursor() as cur:
        delete_query = "DELETE FROM customers WHERE "
        conditions = []
        params = []

        if first_name is not None:
            conditions.append("first_name = %s")
            params.append(first_name)
        if last_name is not None:
            conditions.append("last_name = %s")
            params.append(last_name)
        if email is not None:
            conditions.append("email = %s")
            params.append(email)
        if phone is not None:
            conditions.append("phones @> ARRAY[%s]::VARCHAR[]")
            params.append(phone)

        # Проверка, что хотя бы один параметр для удаления был указан
        if not conditions:
            print("Не указаны параметры для удаления клиента.")
            return

        delete_query += " AND ".join(conditions) + ";"
        cur.execute(delete_query, tuple(params))
        conn.commit()


def find_client(conn, first_name=None, last_name=None, email=None, phone=None):
    """
    ищем клиента
    """
    with conn.cursor() as cur:
        search_query = "SELECT * FROM customers WHERE "
        conditions = []
        params = []

        if first_name is not None:
            conditions.append("first_name = %s")
            params.append(first_name)
        if last_name is not None:
            conditions.append("last_name = %s")
            params.append(last_name)
        if email is not None:
            conditions.append("email = %s")
            params.append(email)
        if phone is not None:
            conditions.append("phones @> ARRAY[%s]::VARCHAR[]")
            params.append(phone)

        if not conditions:
            print("Не указаны параметры для поиска клиента.")
            return

        search_query += " AND ".join(conditions) + ";"

        cur.execute(search_query, tuple(params))
        result = cur.fetchall()
        print(f'найден клиент с id {result[0][0]}')

        return result


# создаем таблицу
create_table(conn)

# добавляем клиентов
add_client(conn, 'Ivan', 'Ivanov', '123@mail.ru')
add_client(conn, 'Sergei', 'Klonov', '12332@mail.ru')
add_client(conn, 'Nina', 'Morozova', 'nina@gmail.com', ['+7988999999'])
add_client(conn, 'Pavel', 'Sergeev', 'pavel@icloud.com', ['+7999000000'])
add_client(conn, 'Fedor', 'Averin', 'fedor_batman@gmail.com')


# добавляем номера к существуюшим клиентам по id
add_phones(conn, 1, ['+7912111111'])
add_phones(conn, 1, ['+79223452200'])
add_phones(conn, 2, ['+79222222222'])

# меняем данные
change_client(conn, 2, 'Vlad', phones=['+91231283232'])

# удаляем номер телефона
del_phone(conn, 5, '+7988999999')

#удалим клиента
del_client(conn, 'Fedor')

# найдем клеинта
find_client(conn, phone='+7912111111')

conn.close()
