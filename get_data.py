import asyncio
import os
from playwright.async_api import async_playwright

async def get_datasus_data(url, group_name, content_label, periods, target_path):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()
        
        print(f"Extraindo {content_label} para {group_name}...")
        try:
            await page.goto(url, timeout=120000, wait_until="networkidle")
            
            # Linha: Município
            try:
                await page.select_option('select#L', label='Município')
            except:
                await page.select_option('select#L', index=0)
            
            # Coluna: Subgrupo proced.
            try:
                await page.select_option('select#C', label='Subgrupo proced.')
            except:
                options = await page.query_selector_all('select#C option')
                for opt in options:
                    text = await opt.inner_text()
                    if 'Subgrupo' in text:
                        val = await opt.get_attribute('value')
                        await page.select_option('select#C', value=val)
                        break

            # Conteúdo
            content_options = await page.query_selector_all('select#I option')
            selected_val = None
            for opt in content_options:
                text = await opt.inner_text()
                if content_label.lower() in text.lower() or ("quant" in content_label.lower() and "qtd" in text.lower()):
                    selected_val = await opt.get_attribute('value')
                    break
            
            if selected_val:
                await page.select_option('select#I', value=selected_val)
            else:
                print(f"Erro: {content_label} não encontrado.")
                await browser.close()
                return

            # Períodos - Reduzir para 12 meses para evitar timeout
            period_options = await page.query_selector_all('select#A option')
            available_period_texts = [await opt.inner_text() for opt in period_options]
            to_select_periods = [p for p in periods if p in available_period_texts][:12]
            
            await page.select_option('select#A', label=to_select_periods)
            
            # Opções Adicionais
            try: await page.check('input#Z')
            except: pass
            await page.click('input[value="prn"]')
            
            # Submeter
            async with context.expect_page(timeout=300000) as new_page_info:
                await page.click('input[type="submit"]')
            new_page = await new_page_info.value
            await new_page.wait_for_load_state("networkidle", timeout=300000)
            await asyncio.sleep(10) # Espera extra
            
            content = await new_page.inner_text('body')
            if "Múltiplos conteúdos" in content:
                print("Erro detectado na página de resultado: Múltiplos conteúdos.")
                await browser.close()
                return

            with open(target_path, "w", encoding="iso-8859-1") as f:
                f.write(content)
            print(f"Sucesso: {target_path} salvo ({len(content)} bytes)")
            
        except Exception as e:
            print(f"Falha em {content_label}: {e}")
        
        await browser.close()

async def main():
    periods = ["Jan/2025", "Dez/2024", "Nov/2024", "Out/2024", "Set/2024", "Ago/2024", "Jul/2024", "Jun/2024", "Mai/2024", "Abr/2024", "Mar/2024", "Fev/2024"]
    
    sih_url = "http://tabnet.datasus.gov.br/cgi/deftohtm.exe?sih/cnv/spabr.def"
    sia_url = "http://tabnet.datasus.gov.br/cgi/deftohtm.exe?sia/cnv/qabr.def"
    
    # Extrair um por um
    await get_datasus_data(sih_url, "sih", "Quantidade aprovada", periods, "/home/ubuntu/data_sih_qtd.csv")
    await asyncio.sleep(5)
    await get_datasus_data(sih_url, "sih", "Valor aprovado", periods, "/home/ubuntu/data_sih_valor.csv")
    await asyncio.sleep(5)
    await get_datasus_data(sia_url, "sia", "Quantidade aprovada", periods, "/home/ubuntu/data_sia_qtd.csv")
    await asyncio.sleep(5)
    await get_datasus_data(sia_url, "sia", "Valor aprovado", periods, "/home/ubuntu/data_sia_valor.csv")

if __name__ == "__main__":
    asyncio.run(main())
