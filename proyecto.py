import sys
import os

scripts_py_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'scripts_py')
sys.path.append(scripts_py_path)

from scripts_py import main_local_data

if __name__ == "__main__":
    main_local_data.main()
    print("Proceso finalizado!")

