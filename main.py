import requests
import mysql.connector
import telebot
import time
import json
import logging

from config import *

logging.basicConfig(filename='log.log',level=logging.DEBUG)


def updateItarationStatus():
    db = mysql.connector.connect(**config)
    cursor = db.cursor(dictionary=True)
    sql = "SELECT number, stamp, status, timediffminutes FROM iterations WHERE id = (select max(id) FROM iterations);"
    cursor.execute(sql)
    rows = cursor.fetchall()
    logging.debug("updateItarationStatus: " + str(rows))

    return rows

def findUncheckedWallets(iteration_id, limit = 10000000):
    db = mysql.connector.connect(**config)
    cursor = db.cursor()
    sql = "SELECT * FROM wallets WHERE iteration_id != " + str(iteration_id) + " LIMIT " + str(limit) + ";"
    cursor.execute(sql)
    rows = cursor.fetchall()

    logging.debug('findUncheckedWallets:' + str(rows))

    return rows

def checkBalance(wallets, iteration_id):
    sql_var_arr = list()
    for wallet in wallets:
        response = requests.get("https://bitaps.com/api/address/" + wallet[1])
        print(response.content)
        wallet_info = json.loads(response.content)
        balance = int(wallet_info['balance']) / 100000000
        diff = int(wallet_info['balance']) / int(100000000) - int(wallet[3])
        sql_var_arr.append([balance, diff, str(int(time.time())), iteration_id, wallet[1]])
    logging.debug('checkBalance:' + str(sql_var_arr))

    return sql_var_arr

def saveWalletsCheckResult(sql_var_arr):
    db = mysql.connector.connect(**config)
    cursor = db.cursor()
    sql = "UPDATE wallets SET balance = %s, diff = %s, time = %s, iteration_id = %s WHERE wallet = %s;"
    cursor.executemany(sql, sql_var_arr)
    result = db.commit()
    cursor.close()
    db.close()
    logging.debug('saveWalletsCheckResult, db.commit(): ' + str(result))

def findDiffs():
    db = mysql.connector.connect(**config)
    cursor = db.cursor()
    sql = "SELECT name, sum(diff) FROM wallets WHERE diff != 0 GROUP BY name;"
    cursor.execute(sql)
    rows = cursor.fetchall()
    logging.debug('findDiffs:' + str(rows))

    return rows

def send_notification(wallets_with_diff):
    if wallets_with_diff:
        text = "⚠ Some balances has changed:\n"
        for wallet in wallets_with_diff:
            text += str(wallet[0]) + ": " + str(wallet[1]) + "\n"
        bot = telebot.TeleBot(BOT_TOKEN)
        result = bot.send_message(CHANNEL_NAME, text)
        logging.debug('send_notification, bot.send_message: ' + str(result))
    else:
        logging.debug('send_notification: Не о чем уведомлять')


def markIterationAsFinished(iteration_id):
    db = mysql.connector.connect(**config)
    cursor = db.cursor()
    sql = "SELECT number, stamp FROM iterations WHERE number = (SELECT max(number) FROM iterations);"
    cursor.execute(sql)
    rows = cursor.fetchone()
    timediffminutes = (int(time.time()) - int(rows[1])) / 60

    sql = "INSERT INTO iterations (number, stamp, timediffminutes, status) VALUES (%s, %s, %s, %s);"
    sql_var_arr = [iteration_id, str(int(time.time())), timediffminutes, 'finish']
    cursor.execute(sql, sql_var_arr)
    result = db.commit()
    cursor.close()
    db.close()
    logging.debug('markIterationAsFinished, db.commit() ' + str(result))

def markIterationAsStarted(iteration_id):
    db = mysql.connector.connect(**config)
    cursor = db.cursor()
    sql = "INSERT INTO iterations (number, stamp, status) VALUES (%s, %s, %s);"
    sql_var_arr = [iteration_id, str(int(time.time())), 'start']
    cursor.execute(sql, sql_var_arr)
    result = db.commit()
    cursor.close()
    db.close()
    logging.debug('markIterationAsStarted, db.commit() ' + str(result))

logging.debug("Program start")
last_iteration = updateItarationStatus()
last_iteration = last_iteration[0]

if last_iteration['status'] == 'finish':
    iteration_number = int(last_iteration['number']) + 1
    markIterationAsStarted(iteration_number)
else:
    iteration_number = int(last_iteration['number'])

while findUncheckedWallets(iteration_number, 1):
    wallets_check_result = checkBalance(findUncheckedWallets(iteration_number, 20), iteration_number)
    saveWalletsCheckResult(wallets_check_result)

if not findUncheckedWallets(iteration_number, 1):
    wallets_with_diff = findDiffs()
    send_notification(wallets_with_diff)
    markIterationAsFinished(iteration_number)
