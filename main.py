import inspect
from pyspark.sql import SparkSession

from common.context import populate_context
import reports


spark = SparkSession.builder.getOrCreate()

context = {}
populate_context(spark, context)

members = inspect.getmembers(reports)
functions = [member for member in members if inspect.isfunction(member[1])]
function_names = [func[0] for func in functions]

for function_name in function_names:
    func = getattr(reports, function_name)
    func(context).write.csv(f'reports/{function_name}')
