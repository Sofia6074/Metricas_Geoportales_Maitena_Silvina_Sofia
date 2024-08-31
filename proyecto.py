import sys
import os
from scripts_py.kaggle_data import main_kaggle_data

scripts_py_path = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), 'scripts_py')
sys.path.append(scripts_py_path)

if __name__ == "__main__":
    # print("# - - - DATOS LOCALES")
    # main_local_data.main()

    print("# - - - DATOS DESDE KAGGLE")
    main_kaggle_data.main()

    print("# - - - PROCESO FINALIZADO")
