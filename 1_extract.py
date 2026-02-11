import threading
from extracters import n28hse, midland, house730

def main():
    thread_n28hse_i = threading.Thread(target=n28hse.extract, args=())
    #thread_midland_i = threading.Thread(target=midland.extract, args=())
    thread_house730_i = threading.Thread(target=house730.extract, args=())
    
    thread_n28hse_i.start()
    #thread_midland_i.start()
    thread_house730_i.start()

    thread_n28hse_i.join()
    #thread_midland_i.join()
    thread_house730_i.join()

if __name__ == "__main__":
    main()