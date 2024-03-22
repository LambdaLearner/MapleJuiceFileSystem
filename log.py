import logging

def setup_logger(name, vm_id):

    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)  

    c_handler = logging.StreamHandler()
    output_file_name = f"log_file_{vm_id[len('fa23-cs425-82') : len('fa23-cs425-82') + 2]}.log"
    f_handler = logging.FileHandler(output_file_name)
    c_handler.setLevel(logging.INFO)  
    f_handler.setLevel(logging.DEBUG)

    c_format = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
    f_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    c_handler.setFormatter(c_format)
    f_handler.setFormatter(f_format)

    logger.addHandler(c_handler)
    logger.addHandler(f_handler)

    return logger
