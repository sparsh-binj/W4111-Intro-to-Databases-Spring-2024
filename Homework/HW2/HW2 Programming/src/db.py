from typing import Any, Dict, List, Tuple, Union

import pymysql

# Type definitions
# Key-value pairs
KV = Dict[str, Any]
# A Query consists of a string (possibly with placeholders) and a list of values to be put in the placeholders
Query = Tuple[str, List]

class DB:
	def __init__(self, host: str, port: int, user: str, password: str, database: str):
		conn = pymysql.connect(
			host=host,
			port=port,
			user=user,
			password=password,
			database=database,
			cursorclass=pymysql.cursors.DictCursor,
			autocommit=True,
		)
		self.conn = conn

	def get_cursor(self):
		return self.conn.cursor()

	def execute_query(self, query: str, args: List, ret_result: bool) -> Union[List[KV], int]:
		"""Executes a query.

		:param query: A query string, possibly containing %s placeholders
		:param args: A list containing the values for the %s placeholders
		:param ret_result: If True, execute_query returns a list of dicts, each representing a returned
							row from the table. If False, the number of rows affected is returned. Note
							that the length of the list of dicts is not necessarily equal to the number
							of rows affected.
		:returns: a list of dicts or a number, depending on ret_result
		"""
		cur = self.get_cursor()
		count = cur.execute(query, args=args)
		if ret_result:
			return cur.fetchall()
		else:
			return count


	# TODO: all methods below


	@staticmethod
	def build_select_query(table: str, columns: List[str], filters: KV) -> Query:
		"""Builds a query that selects rows. See db_test for examples.

		:param table: The table to be selected from
		:param columns: The attributes to select. If empty, then selects all columns.
		:param filters: Key-value pairs that the rows from table must satisfy
		:returns: A query string and any placeholder arguments
		"""

		attrib_clause = "*" if not rows else ", ".join(map(str, rows))
		where_clause = "" if not filters else " WHERE " + " AND ".join([f"{keyword} = %s" for keyword in filters.keys()])
		args = list(filters.values())
		return "SELECT " + attrib_clause + f" FROM {table}" + where_clause, args


	def select(self, table: str, columns: List[str], filters: KV) -> List[KV]:
		"""Runs a select statement. You should use build_select_query and execute_query.

		:param table: The table to be selected from
		:param columns: The attributes to select. If empty, then selects all columns.
		:param filters: Key-value pairs that the rows to be selected must satisfy
		:returns: The selected rows
		"""
		query, args = self.build_select_query(table, rows, filters)
		result = self.execute_query(query, args, True)
		return result


	@staticmethod
	def build_insert_query(table: str, values: KV) -> Query:
		"""Builds a query that inserts a row. See db_test for examples.

		:param table: The table to be inserted into
		:param values: Key-value pairs that represent the values to be inserted
		:returns: A query string and any placeholder arguments
		"""
		attrib_clause = " (" + ", ".join(map(str, values.keys())) + ")"
		args = list(values.values())
		placeholder = ["%s"]*len(args)
		values_clause = " VALUES (" + ", ".join(map(str, placeholder)) + ")"
		return f"INSERT INTO {table}" + attrib_clause + values_clause, args

	def insert(self, table: str, values: KV) -> int:
		"""Runs an insert statement. You should use build_insert_query and execute_query.

		:param table: The table to be inserted into
		:param values: Key-value pairs that represent the values to be inserted
		:returns: The number of rows affected
		"""
		query, args = self.build_insert_query(table, values)
		result = self.execute_query(query, args, False)
		return result

	@staticmethod
	def build_update_query(table: str, values: KV, filters: KV) -> Query:
		"""Builds a query that updates rows. See db_test for examples.

		:param table: The table to be updated
		:param values: Key-value pairs that represent the new values
		:param filters: Key-value pairs that the rows from table must satisfy
		:returns: A query string and any placeholder arguments
		"""
		set_clause = "SET " + ", ".join([f"{keyword} = %s" for keyword in values.keys()])
		where_clause = "" if not filters else " WHERE " + " AND ".join([f"{keyword} = %s" for keyword in filters.keys()])
		args = list(values.values()) + list(filters.values())
		return f"UPDATE {table} " + set_clause + where_clause, args

	def update(self, table: str, values: KV, filters: KV) -> int:
		"""Runs an update statement. You should use build_update_query and execute_query.

		:param table: The table to be updated
		:param values: Key-value pairs that represent the new values
		:param filters: Key-value pairs that the rows to be updated must satisfy
		:returns: The number of rows affected
		"""
		query, args = self.build_update_query(table, values, filters)
		result = self.execute_query(query, args, False)
		return result

	@staticmethod
	def build_delete_query(table: str, filters: KV) -> Query:
		"""Builds a query that deletes rows. See db_test for examples.

		:param table: The table to be deleted from
		:param filters: Key-value pairs that the rows to be deleted must satisfy
		:returns: A query string and any placeholder arguments
		"""
		where_clause = "" if not filters else " WHERE " + " AND ".join([f"{keyword} = %s" for keyword in filters.keys()])
		args = list(filters.values())
		return f"DELETE FROM {table}" + where_clause, args

	def delete(self, table: str, filters: KV) -> int:
		"""Runs a delete statement. You should use build_delete_query and execute_query.

		:param table: The table to be deleted from
		:param filters: Key-value pairs that the rows to be deleted must satisfy
		:returns: The number of rows affected
		"""
		query, args = self.build_delete_query(table, filters)
		result = self.execute_query(query, args, False)
		return result
