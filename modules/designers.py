from unidecode import unidecode
import os
import re
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
import requests
from bs4 import BeautifulSoup
import pandas as pd
import logging
from modules.boardgame import save_games
from config.db_connection import run_query, df_to_db
from dotenv_vault import load_dotenv

load_dotenv()
logger = logging.getLogger()
logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, os.getenv("LOG_LEVEL", "INFO")))

chrome_options = Options()
chrome_options.add_argument("--headless")
chrome_options.add_argument("--log-level=OFF")


def get_designers(url):
    url_page = url + "/page/21"
    driver = webdriver.Chrome(options=chrome_options)
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
        already_in_db = run_query(f"select id from boardgames.designer")
        designers_dict = {
            int(k["href"].split("/")[2]): k.string
            for k in designers_html
            if int(k["href"].split("/")[2]) not in already_in_db.id.to_list()
        }
        designers_df = pd.DataFrame(designers_dict.items(), columns=["id", "name"])
        df_to_db(designers_df, "designer", "boardgames")
        get_games_from_designer(
            designers_df.id.to_list(),
            designers_df.name.to_list(),
            only_total=True,
        )
    logger.info(f"Iteration complete on {str(i)} pages")
    driver.quit()


def get_games_from_designer(
    id_list: list,
    name_list: list = None,
    only_total: bool = False,
    verbose: bool = False,
):
    id_list_str = [str(x) for x in id_list]
    if name_list is not None:
        name_list = run_query(
            f"""select name from boardgames.designer where id in ({",".join(id_list_str)})"""
        ).name.to_list()
    count_designer = len(name_list)
    if verbose:
        logger.info("###################################################")
        logger.info(f"Processing {count_designer} designers")
        logger.info("###################################################")
    i = 0
    browser = webdriver.Chrome(options=chrome_options)
    for id, designer in zip(id_list, name_list):
        i += 1
        if verbose:
            logger.info("###################################################")
            logger.info(
                f"Designer {str(i).zfill(2)}/{str(count_designer).zfill(2)}: Processing {designer}({id})"
            )
            logger.info("###################################################")

        regex = re.compile("[^\w-]")
        new_name = regex.sub("", unidecode(designer).replace(" ", "-")).lower()
        url_designer = f"https://boardgamegeek.com/boardgamedesigner/{id}/{new_name}"
        url_best_rank_games = "/linkeditems/boardgamedesigner?pageid=1&sort=rank"

        browser.get(url_designer + url_best_rank_games)
        time.sleep(2)
        html = browser.page_source

        bs = BeautifulSoup(html, features="html.parser")
        designer_games_url = bs.findAll("div", class_="media-left")
        total_games = int(
            re.sub("[^0-9]", "", bs.find("strong", class_="ng-binding").text)
        )
        designer_games = [
            game_url.find("a")["href"].split("/")[2] for game_url in designer_games_url
        ]
        if len(designer_games) == 0:
            logger.warning(f"Designer {designer} does not have any games")
        else:
            run_query(
                f"update boardgames.designer set total_games = {int(total_games)} where id = {int(id)}",
                execute_only=True,
            )
            if not only_total:
                save_games(designer_games)
    browser.quit()
