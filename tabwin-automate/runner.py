import subprocess
from moverfiles import root_dbf, destiny_dbf, destiny_sql
from moverfiles import list_files, move_files
from logger import write_log
import pyautogui
import os
import time


arquivos_e_diretorios = list_files(root_dbf)

print(len(arquivos_e_diretorios))
time.sleep(2)


# Abrir o TABWIN
processo_tabwin = subprocess.Popen(r"C:\Users\jmoni\OneDrive\Área de Trabalho\TABWIN\TabWin415.exe")
pyautogui.sleep(2)


times = 0
while len(arquivos_e_diretorios) != 0:
    pyautogui.click(25, 33)    #1.
    pyautogui.sleep(0.3)
    pyautogui.click(118, 262)  #2.
    pyautogui.sleep(0.3)
    pyautogui.click(713, 365)  #3.
    pyautogui.sleep(0.3)
    pyautogui.click(747, 392)  #4.
    pyautogui.sleep(0.3)
    pyautogui.doubleClick(833, 498)  #5.
    pyautogui.sleep(2)
    # pyautogui.click(1121, 495) #6.
    # pyautogui.sleep(3)
    pyautogui.click(813, 384)  #7.
    pyautogui.sleep(6)
    pyautogui.click(1206, 337)  #8.

    time.sleep(2)

    arquivos_e_diretorios = list_files(root_dbf)
    try:
        for file in arquivos_e_diretorios:
            if file.endswith(".sql"):
                transfer_sql = file
                break
        for file in arquivos_e_diretorios:
            if os.path.splitext(file)[0] == os.path.splitext(transfer_sql)[0]:
                transfer_dbf = file
                break

        move_files(os.path.join(root_dbf, transfer_dbf), destiny_dbf)
        move_files(os.path.join(root_dbf, transfer_sql), destiny_sql)

        write_log(transfer_dbf, transfer_sql)
    except Exception as e:
        write_log(str(e))

    times += 1

    if times >= 15:
        break
# para o loop
# pyautogui.click(747, 392)  #4.
# pyautogui.sleep(0.3)
# pyautogui.click(840, 370)  #5.
# pyautogui.sleep(0.3)
# pyautogui.click(1121, 495) #6.
# pyautogui.sleep(0.3)
# pyautogui.click(813, 384)  #7.
# pyautogui.sleep(0.3)

processo_tabwin.kill()
