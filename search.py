import os
import re

base_dir = r"c:\Users\vikas\Downloads\Osha\OshaBackend"
out_file = r"c:\Users\vikas\Downloads\Osha\OshaBackend\results.txt"

with open(out_file, "w", encoding="utf-8") as out:
    for root, dirs, files in os.walk(base_dir):
        for file in files:
            if file == "lambda_function.py":
                filepath = os.path.join(root, file)
                with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
                    lines = f.readlines()
                
                in_data_loader = False
                current_func = ""
                for i, line in enumerate(lines):
                    if line.strip().startswith("def "):
                        current_func = line.strip()
                        if "load" in current_func or "get_inspection" in current_func:
                            in_data_loader = True
                        else:
                            in_data_loader = False
                            
                    if in_data_loader and ".get_item(" in line and "ConsistentRead=True" not in line:
                        out.write(f"File: {filepath}\n")
                        out.write(f"Line: {i+1}\n")
                        out.write(f"Func: {current_func}\n")
                        out.write(f"Content: {line}\n")
                        out.write("-" * 40 + "\n")
