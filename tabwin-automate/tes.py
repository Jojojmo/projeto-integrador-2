import subprocess
from moverfiles import root_dbf, destiny_dbf, destiny_sql
from moverfiles import list_files, move_files
from logger import write_log
import pyautogui
import os
import time

# Abrir o TABWIN
processo_tabwin = subprocess.Popen(r"C:\Users\jmoni\OneDrive\Área de Trabalho\TABWIN\TabWin415.exe")
pyautogui.sleep(2)

arquivos_e_diretorios = list_files(root_dbf)


pyautogui.click(25, 33)    #1.
pyautogui.sleep(0.3)
pyautogui.click(118, 262)  #2.
pyautogui.sleep(0.3)
pyautogui.click(713, 365)  #3.
pyautogui.sleep(0.3)
pyautogui.click(747, 392)  #4.
pyautogui.sleep(5)
pyautogui.doubleClick(833, 498)  #5.
pyautogui.sleep(2)
# pyautogui.click(1121, 495) #6.
# pyautogui.sleep(3)
pyautogui.click(813, 384)  #7.
pyautogui.sleep(10)
pyautogui.click(1206, 337)  #8.