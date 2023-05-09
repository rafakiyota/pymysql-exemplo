"""
Módulo para testar banco de dados pymysql
"""

import os
import json
import logging
from datetime import datetime, timezone
import pymysql


DB_HOST = os.environ['DB_HOST']
DB_USER = os.environ['DB_USER']
DB_PW = os.environ['DB_PW']
DB_SCHEMA = os.environ['DB_SCHEMA']


conexao = pymysql.connect(
    host=DB_HOST,
    user=DB_USER,
    password=DB_PW,
    db=DB_SCHEMA
)


def valida_usuario(email):
    """
    valida se o usuario ja foi cadastrado
    """

    cursor = conexao.cursor(pymysql.cursors.DictCursor)

    query = "SELECT * FROM users WHERE email = %s"
    escape_email = (email,)
    cursor.execute(query, escape_email)
    usuario = cursor.fetchone()

    if usuario:
        raise ValueError(f'Usuario {email} ja esta cadastrado no sistema!!!')


def add_usuario(nome, email):
    """
    Add um novo usuario
    """

    cursor = conexao.cursor(pymysql.cursors.DictCursor)

    query = "INSERT INTO users (name, email) VALUES (%s, %s)"
    valores = (nome, email)
    cursor.execute(query, valores)

    query = "SELECT * FROM users WHERE id = %s"
    usuario_id = (conexao.insert_id(),)
    cursor.execute(query, usuario_id)
    return cursor.fetchone()


def monta_response(status, body):
    """
    Monta o response
    """

    if isinstance(body, dict):
        timestamp = datetime.now(timezone.utc).astimezone().isoformat()
        body['timestamp'] = timestamp

    return {
        'statusCode': status,
        'body': json.dumps(body),
        'headers': {
            'Content-Type': 'application/json'
        }
    }


def finaliza_cadastro_usuario(nome, email):
    """
    Finaliza cadastro de usuario commitando e fechando conexao
    """

    try:
        valida_usuario(email)
        novo_usuario = add_usuario(nome, email)
        response = monta_response(200, novo_usuario)
        conexao.commit()

    except ValueError as erro:
        msg = {'msg': str(erro)}
        response = monta_response(404, msg)
        logging.exception(erro)

    except Exception as erro:
        conexao.rollback()
        msg = {'msg': str(erro)}
        response = monta_response(500, msg)
        logging.exception(erro)

    finally:
        conexao.close()

    return response


if __name__ == "__main__":
    new_usuario = finaliza_cadastro_usuario('joao', 'joao@email.com')
    print(new_usuario)
