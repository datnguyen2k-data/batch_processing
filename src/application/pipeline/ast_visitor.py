import pyspark.sql.functions as F
from pyspark.sql import Column

class SparkAstVisitor:
    """
    Parses a JSON AST representing an expression and compiles it into PySpark Column operations.
    This entirely decouples the Spark engine from string DSL parsing (like pyparsing or regex).
    """
    
    def visit(self, ast_dict: dict) -> Column:
        if not isinstance(ast_dict, dict) or "type" not in ast_dict:
            raise ValueError(f"Invalid AST Node: {ast_dict}")

        node_type = ast_dict["type"]
        
        if node_type == "ColumnRef":
            return F.col(ast_dict["name"])
            
        elif node_type == "Number":
            return F.lit(ast_dict["value"])
            
        elif node_type == "String":
            return F.lit(ast_dict["value"])
            
        elif node_type == "BinaryOp":
            left_col = self.visit(ast_dict["left"])
            right_col = self.visit(ast_dict["right"])
            op = ast_dict["op"]
            
            if op == "+": return left_col + right_col
            elif op == "-": return left_col - right_col
            elif op == "*": return left_col * right_col
            elif op == "/": return left_col / right_col
            elif op == "=" or op == "==": return left_col == right_col
            elif op == ">": return left_col > right_col
            elif op == "<": return left_col < right_col
            elif op == ">=": return left_col >= right_col
            elif op == "<=": return left_col <= right_col
            elif op == "!=" or op == "<>": return left_col != right_col
            elif op.upper() == "AND": return left_col & right_col
            elif op.upper() == "OR": return left_col | right_col
            else:
                raise NotImplementedError(f"Unsupported BinaryOp: {op}")
                
        elif node_type == "FunctionCall":
            func_name = ast_dict["name"].upper()
            args = ast_dict.get("args", [])
            compiled_args = [self.visit(arg) for arg in args]
            
            # Tableau-like functions mapping to PySpark
            if func_name == "IIF" or func_name == "IF":
                if len(compiled_args) != 3:
                    raise ValueError(f"{func_name} requires 3 arguments: condition, true_val, false_val")
                return F.when(compiled_args[0], compiled_args[1]).otherwise(compiled_args[2])
                
            elif func_name == "IFNULL" or func_name == "ISNULL" or func_name == "NVL":
                if len(compiled_args) != 2:
                    raise ValueError(f"{func_name} requires 2 arguments")
                return F.coalesce(compiled_args[0], compiled_args[1])
                
            elif func_name == "UPPER":
                return F.upper(compiled_args[0])
                
            elif func_name == "LOWER":
                return F.lower(compiled_args[0])
                
            elif func_name == "LEFT":
                return F.substring(compiled_args[0], 1, compiled_args[1])
                
            elif func_name == "SPLIT":
                # SPLIT(string, delimiter, index) -> in PySpark split()[index]
                if len(compiled_args) == 3:
                     # Note: Spark split returns an array (1-indexed based or 0-indexed in array element extractor)
                     # getItem(0) because Spark Array is 0-indexed via getItem
                     idx = args[2]["value"] # we just assume it's a Number literal for extraction
                     return F.split(compiled_args[0], compiled_args[1]).getItem(idx)
                return F.split(compiled_args[0], compiled_args[1])
                
            elif func_name == "CONCAT":
                return F.concat(*compiled_args)
                
            else:
                raise NotImplementedError(f"Unsupported FunctionCall: {func_name}")
                
        else:
             raise NotImplementedError(f"Unsupported AST Node Type: {node_type}")
