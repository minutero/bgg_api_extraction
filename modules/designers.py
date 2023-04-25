from unidecode import unidecode
import os
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
import requests
from bs4 import BeautifulSoup
import pandas as pd
import logging
from modules.db import create_connection, run_query, check_exists_db
from modules.boardgame import boardgame
from modules.helper import save_games

logger = logging.getLogger()
logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, os.getenv("LOG_LEVEL", "INFO")))

def get_designers(url):
    url_page = url + "/page/21"
    driver = webdriver.Chrome()
    time.sleep(2)
    driver.get(url_page)
    driver.find_element(By.ID, "inputUsername").send_keys(os.environ.get("bgg_user"))
    driver.find_element(By.ID, "inputPassword").send_keys(os.environ.get("bgg_pass"))
    driver.find_element(
        By.XPATH,
        '//*[@id="mainbody"]/div/div/gg-login-page/div[1]/div/gg-login-form/form/fieldset/div[3]/button[1]',
    ).click()

    df_list = []
    for i in range(1, 186):
        url_page = url + "/page/" + str(i)
        if i >= 21:
            driver.get(url_page)
            html = driver.page_source
            if i == 21:
                logger.info("Starting with private pages")
        else:
            req = requests.get(url_page)
            html = req.content
        bs = BeautifulSoup(html)
        designers_html = bs.table.findAll(
            lambda tag: tag.name == "a" and tag.has_attr("href")
        )
        designers_dict = {
            k["href"].split("/")[2]: v.string
            for k, v in zip(designers_html, designers_html)
        }
        designers_df = pd.DataFrame(designers_dict.items(), columns=["id", "designer"])
        df_list.append(designers_df.astype({"id": "int32", "designer": "str"}))
    logger.info(f"Iteration complete on {str(i)} pages")
    df = pd.concat(df_list).drop_duplicates()
    df.to_sql(
        "designers",
        create_connection(),
        index=False,
        if_exists="append",
    )
    driver.quit()


def get_games_from_designer(name, designer_id=None):
    if not designer_id:
        id = run_query(
            f"select id from designers where lower(designer) = '{name.lower()}'",
            execute_only=False,
        ).loc[0][0]
    else:
        id = designer_id

    url_designer = f"https://boardgamegeek.com/boardgamedesigner/{id}/{unidecode(name).lower().replace(' ','-')}"
    url_best_rank_games = "/linkeditems/boardgamedesigner?pageid=1&sort=average"
    driver = webdriver.Chrome()
    time.sleep(2)
    driver.get(url_designer + url_best_rank_games)
    html = driver.page_source
    driver.quit()

    bs = BeautifulSoup(html)
    designer_games_url = bs.findAll("div", class_="media-left")
    designer_games = [
        game_url.find("a")["href"].split("/")[2]
        for game_url in designer_games_url
    ]
    save_games(designer_games)