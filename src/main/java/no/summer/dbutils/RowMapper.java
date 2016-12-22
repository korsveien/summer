package no.summer.dbutils;

import java.sql.ResultSet;
import java.sql.SQLException;

public interface RowMapper<T> {

	T map(ResultSet rs) throws SQLException;
	
	class StringMapper implements RowMapper<String> {
		public String map(ResultSet rs) throws SQLException {
			return rs.getString(1);
		}
	}

	class IntMapper implements RowMapper<Integer> {
		public Integer map(ResultSet rs) throws SQLException {
			return rs.getInt(1);
		}
	}
	
	class LongMapper implements RowMapper<Long> {
		public Long map(ResultSet rs) throws SQLException {
			return rs.getLong(1);
		}
	}

}
