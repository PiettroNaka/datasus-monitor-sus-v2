from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
import pandas as pd
import time
import os
import re

TIMEOUT = 30_000  # em ms
MAX_RETRIES = 3
BASE_URL = "http://tabnet.datasus.gov.br/cgi/deftohtm.exe?sih/cnv/spabr.def"


def safe_filename(text: str) -> str:
    text = str(text).strip()
    text = re.sub(r'[\\/*?:"<>|]', "_", text)
    text = re.sub(r"\s+", " ", text)
    return text


def wait_and_click(page, xpath: str):
    locator = page.locator(f"xpath={xpath}")
    locator.wait_for(state="visible", timeout=TIMEOUT)
    locator.click(timeout=TIMEOUT)
    return locator


def wait_and_find(page, xpath: str):
    locator = page.locator(f"xpath={xpath}")
    locator.wait_for(state="visible", timeout=TIMEOUT)
    return locator


def get_text(page, xpath: str) -> str:
    locator = wait_and_find(page, xpath)
    txt = locator.text_content()
    return (txt or "").strip()


def get_data(meses: int = 13):
    os.makedirs("baixados", exist_ok=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context()
        page = context.new_page()

        page.goto(BASE_URL, wait_until="domcontentloaded", timeout=60_000)

        # Esperar o formulário carregar
        wait_and_find(page, '//*[@id="I"]')

        # Desselecionar/configurar página
        # Conteúdo
        wait_and_click(page, '//*[@id="I"]/option[1]')
        # Mês
        wait_and_click(page, '//*[@id="A"]/option[1]')
        # Exibir linhas zeradas
        wait_and_click(page, '//*[@id="Z"]')
        # Separador ;
        wait_and_click(page, '/html/body/div/div/center/div/form/div[4]/div[2]/div[1]/div[2]/input[3]')

        for coluna_xpath in ['//*[@id="C"]/option[7]', '//*[@id="C"]/option[8]']:
            coluna_nome = get_text(page, coluna_xpath)
            wait_and_click(page, coluna_xpath)

            for conteudo_xpath in ['//*[@id="I"]/option[1]', '//*[@id="I"]/option[2]']:
                conteudo_nome = get_text(page, conteudo_xpath)
                wait_and_click(page, conteudo_xpath)

                df_temp = pd.DataFrame()

                for mes in range(1, meses + 1):
                    mes_xpath = f'//*[@id="A"]/option[{mes}]'
                    wait_and_click(page, mes_xpath)
                    ano = get_text(page, mes_xpath)

                    submit_xpath = '/html/body/div/div/center/div/form/div[4]/div[2]/div[2]/input[1]'

                    pre_text = None

                    for attempt in range(1, MAX_RETRIES + 1):
                        try:
                            with context.expect_page(timeout=60_000) as new_page_info:
                                wait_and_click(page, submit_xpath)

                            result_page = new_page_info.value
                            result_page.wait_for_load_state("domcontentloaded", timeout=60_000)

                            pre_locator = result_page.locator("pre")
                            pre_locator.wait_for(state="visible", timeout=120_000)

                            result_page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                            time.sleep(1)

                            pre_text = pre_locator.text_content()
                            if pre_text:
                                pre_text = pre_text.strip()
                                break

                            raise PlaywrightTimeoutError("Elemento <pre> vazio ou não encontrado.")

                        except PlaywrightTimeoutError:
                            print(
                                f"[WARN] Timeout ao buscar <pre> "
                                f"(tentativa {attempt}/{MAX_RETRIES}), reiniciando..."
                            )

                            try:
                                if 'result_page' in locals() and not result_page.is_closed():
                                    html = result_page.content()
                                    with open(
                                        f"debug_page_{attempt}.html",
                                        "w",
                                        encoding="utf-8"
                                    ) as f:
                                        f.write(html)
                                    result_page.close()
                            except Exception:
                                pass

                            if attempt == MAX_RETRIES:
                                raise
                        except Exception:
                            try:
                                if 'result_page' in locals() and not result_page.is_closed():
                                    html = result_page.content()
                                    with open(
                                        f"debug_page_{attempt}.html",
                                        "w",
                                        encoding="utf-8"
                                    ) as f:
                                        f.write(html)
                                    result_page.close()
                            except Exception:
                                pass

                            if attempt == MAX_RETRIES:
                                raise

                    if not pre_text:
                        raise RuntimeError("Não foi possível capturar o conteúdo do <pre>.")

                    dados = pre_text.split("\n")
                    df = pd.DataFrame([x.split(";") for x in dados])
                    df["ano"] = ano

                    if df_temp.empty:
                        df_temp = df
                    else:
                        # Mantém a lógica original: ignora cabeçalho e última linha
                        df_temp = pd.concat([df_temp, df.iloc[1:-1]], ignore_index=True)

                    try:
                        if 'result_page' in locals() and not result_page.is_closed():
                            result_page.close()
                    except Exception:
                        pass

                    # Reforça o foco na página principal
                    page.bring_to_front()
                    wait_and_find(page, '//*[@id="A"]')

                arquivo_saida = (
                    f"baixados/{safe_filename(coluna_nome)}_{safe_filename(conteudo_nome)}.csv"
                )
                df_temp.to_csv(arquivo_saida, index=False, sep=";", encoding="utf-8-sig")

                # Desmarca o conteúdo atual para seguir igual à lógica original
                wait_and_click(page, conteudo_xpath)

        browser.close()


if __name__ == "__main__":
    get_data(13)
