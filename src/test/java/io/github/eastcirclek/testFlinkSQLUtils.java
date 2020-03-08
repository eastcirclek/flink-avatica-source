package io.github.eastcirclek;

import org.apache.flink.table.api.DataTypes;
import org.junit.jupiter.api.Test;

import java.sql.JDBCType;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static io.github.eastcirclek.FlinkSQLUtils.jdbcType2FlinkDataType;

public class testFlinkSQLUtils {
    @Test
    void testJdbcType2FlinkType() {
        assertEquals(jdbcType2FlinkDataType(JDBCType.TIMESTAMP.getName()), DataTypes.TIMESTAMP(3));
        assertEquals(jdbcType2FlinkDataType(JDBCType.BIGINT.getName()), DataTypes.BIGINT());
        assertEquals(jdbcType2FlinkDataType(JDBCType.VARCHAR.getName()), DataTypes.STRING());
        assertEquals(jdbcType2FlinkDataType(JDBCType.FLOAT.getName()), DataTypes.FLOAT());
        assertEquals(jdbcType2FlinkDataType(JDBCType.DOUBLE.getName()), DataTypes.DOUBLE());
    }
}
