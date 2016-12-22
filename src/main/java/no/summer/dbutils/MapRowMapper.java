package no.summer.dbutils;

import java.sql.ResultSet;
import java.sql.SQLException;

public interface MapRowMapper<K, V> {

	K getKey(ResultSet rs) throws SQLException;
	V getValue(ResultSet rs) throws SQLException;


}
