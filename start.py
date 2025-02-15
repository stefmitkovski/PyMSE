import os
from main import mongodb_initial,start,download,processing_reports,list_companies   

import os

def main_screen():
    title = "PyMSE"
    options = ["Стартувај(server mode)", "Стартувај преземање на нови извештаи", 
               "Стартувај преработка на извештаи", "Преглед листа компании", 
               "Преглед и едитирање на константи", "Излези"]
    
    terminal_width = os.get_terminal_size().columns
    
    print(f"{title:^{terminal_width}}")
    
    print("=" * terminal_width)
    
    for idx, option in enumerate(options, 1):
        option_line = f"{idx}. {option}"
        print(f"{option_line:^{terminal_width}}")    
    print("=" * terminal_width)
    
    choice = int(input(f"Внеси избор(1-{len(options)}): "))
    return choice


def clear_screen():
    if os.name == 'nt':
        _ = os.system('cls')

    else:
        _ = os.system('clear')

import os
import subprocess
import sys

def edit_env():
    env_file = '.env'
    
    if not os.path.exists(env_file):
        print(f"Фајл {env_file} не постои. Создава нов фајл...")
        with open(env_file, 'w') as file:
            file.write("# .env file\n")
            print(f"Создаден е новиот фајл {env_file}.")

    try:
        if sys.platform == "win32":
            subprocess.run(["notepad", env_file])
        elif sys.platform == "darwin":
            subprocess.run(["open", "-e", env_file])
        else:
            subprocess.run(["xdg-open", env_file])
        
    except Exception as e:
        print(f"Не може да се отвори уредникот за {env_file}. Грешка: {e}")

def main():
    while True:
        try:
            clear_screen()
            choice = main_screen()
            clear_screen()
            if choice == 1:
                print("Сервер Моде")
                start()
                break
            elif choice == 2:
                print("Стартувај преземање на нови извештаи")
                if(mongodb_initial()):
                    download()
                input("Завршено, притисни Enter за да продолжиш\n")
            elif choice == 3:
                print("Стартувај преработка на извештаи")
                if(mongodb_initial()):
                    processing_reports()
                input("Завршено, притисни Enter за да продолжиш\n")
            elif choice == 4:
                print("Преглед листа компании")
                if(mongodb_initial()):
                    companies = list_companies(fromRequest=False)
                    if companies:
                        for company in companies:
                            print(f"{company}")
                input("Завршено, притисни Enter за да продолжиш\n")
                # continue
            elif choice == 5:
                print("Преглед и едитирање на константи")
                if(mongodb_initial()):
                    edit_env()
                input("Завршено, притисни Enter за да продолжиш\n")
            elif choice == 6:
                print("Излез, приајтно !")
                break
            else:
                print("Непостои тој избор, обиде повторно")
                input("Притисни Enter за да продолжиш")
        except:
            print("Грешка, стартувај ја програмата повторно\n")

if __name__ == "__main__":
    main()
