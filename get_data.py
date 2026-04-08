import asyncio
import os
import pandas as pd
import io
import re
from playwright.async_api import async_playwright

async def get_tabnet_data_monthly(url, group_name, months_to_extract=12):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()
        
        print(f"Iniciando extração para {group_name}...")
        try:
            await page.goto(url, timeout=120000, wait_until="networkidle")
        except Exception as e:
            print(f"Erro ao acessar URL: {e}")
            await browser.close()
            return None
        
        # Configurações fixas: Linha (Município), Coluna (Subgrupo)
        try: await page.select_option('select#L', label='Município')
        except: await page.select_option('select#L', index=0)
        
        try: 
            await page.select_option('select#C', label='Subgrupo proced.')
        except: 
            options = await page.query_selector_all('select#C option')
            for opt in options:
                text = await opt.inner_text()
                if 'Subgrupo' in text:
                    await page.select_option('select#C', value=await opt.get_attribute('value'))
                    break

        # Obter lista de conteúdos (Quantidade e Valor)
        content_options = await page.query_selector_all('select#I option')
        contents = []
        for opt in content_options:
            text = await opt.inner_text()
            if 'quantidade' in text.lower() or 'valor' in text.lower() or 'qtd' in text.lower():
                contents.append({'label': text, 'value': await opt.get_attribute('value')})

        # Obter lista de períodos (12 meses)
        period_options = await page.query_selector_all('select#A option')
        periods = []
        for opt in period_options[:months_to_extract]:
            periods.append({'label': await opt.inner_text(), 'value': await opt.get_attribute('value')})

        all_data = []

        for period in periods:
            print(f"Processando período: {period['label']}")
            # Selecionar o mês atual
            await page.select_option('select#A', value=period['value'])
            
            # Extrair cada conteúdo separadamente
            for content in contents:
                print(f"  Extraindo {content['label']}...")
                await page.select_option('select#I', value=content['value'])
                
                # Configurações adicionais
                try: await page.check('input#Z') # Linhas zeradas
                except: pass
                await page.click('input[value="prn"]') # Formato CSV (prn)
                
                try:
                    async with context.expect_page(timeout=120000) as new_page_info:
                        await page.click('input[type="submit"]')
                    new_page = await new_page_info.value
                    await new_page.wait_for_load_state("networkidle", timeout=120000)
                    
                    raw_text = await new_page.inner_text('body')
                    await new_page.close()
                    
                    # Parsing básico do CSV
                    df_temp = parse_raw_tabnet(raw_text)
                    if df_temp is not None:
                        match = re.search(r'(\w{3})/(\d{4})', period['label'])
                        month_str, year_str = match.groups() if match else ("IGN", "0000")
                            
                        df_temp['ANO'] = year_str
                        df_temp['MES'] = month_str
                        df_temp['METRICA'] = 'QTD' if 'quant' in content['label'].lower() or 'qtd' in content['label'].lower() else 'VALOR'
                        all_data.append(df_temp)
                except Exception as e:
                    print(f"  Erro no período {period['label']} ({content['label']}): {e}")

        await browser.close()
        
        if all_data:
            final_df = pd.concat(all_data, ignore_index=True)
            return pivot_datasus_data(final_df)
        return None

def parse_raw_tabnet(text):
    lines = text.split('\n')
    header_idx = -1
    max_sep = 0
    for i, line in enumerate(lines[:50]):
        if line.count(';') > max_sep:
            max_sep = line.count(';')
            header_idx = i
            
    if header_idx == -1: return None
    
    data_lines = []
    for line in lines[header_idx:]:
        if line.count(';') >= max_sep and not ('Total' in line or 'Fonte:' in line):
            data_lines.append(line)
        if 'Fonte:' in line: break
            
    df = pd.read_csv(io.StringIO("\n".join(data_lines)), sep=';', encoding='utf-8', thousands='.', decimal=',')
    return df

def pivot_datasus_data(df):
    mun_col = df.columns[0]
    meta_cols = [mun_col, 'ANO', 'MES', 'METRICA']
    subgroups = [c for c in df.columns if c not in meta_cols]
    
    # Transformar para formato longo
    df_long = df.melt(id_vars=meta_cols, value_vars=subgroups, var_name='SUBGRUPO_NOME', value_name='VALOR_NUM')
    df_long['SUBGRUPO_CODE'] = df_long['SUBGRUPO_NOME'].str.extract(r'(\d{4})')
    df_long['VAR_NAME'] = df_long['METRICA'] + '_' + df_long['SUBGRUPO_CODE'].fillna('TOTAL')
    
    # Pivotar
    df_pivot = df_long.pivot_table(
        index=[mun_col, 'ANO', 'MES'], 
        columns='VAR_NAME', 
        values='VALOR_NUM', 
        aggfunc='sum'
    ).reset_index()
    
    # Calcular Totais
    qtd_cols = [c for c in df_pivot.columns if 'QTD_' in c]
    val_cols = [c for c in df_pivot.columns if 'VALOR_' in c]
    df_pivot['QT_TOTAL'] = df_pivot[qtd_cols].sum(axis=1)
    df_pivot['VL_TOTAL'] = df_pivot[val_cols].sum(axis=1)
    
    return df_pivot

async def main():
    sih_url = "http://tabnet.datasus.gov.br/cgi/deftohtm.exe?sih/cnv/spabr.def"
    sia_url = "http://tabnet.datasus.gov.br/cgi/deftohtm.exe?sia/cnv/qabr.def"
    
    # Extrair 12 meses
    df_sih = await get_tabnet_data_monthly(sih_url, "SIH", months_to_extract=12)
    if df_sih is not None:
        df_sih.to_csv("/home/ubuntu/datasus-v2/sih_formatted.csv", index=False, sep=';')
        print("SIH Formatado salvo.")

    df_sia = await get_tabnet_data_monthly(sia_url, "SIA", months_to_extract=12)
    if df_sia is not None:
        df_sia.to_csv("/home/ubuntu/datasus-v2/sia_formatted.csv", index=False, sep=';')
        print("SIA Formatado salvo.")

if __name__ == "__main__":
    asyncio.run(main())
