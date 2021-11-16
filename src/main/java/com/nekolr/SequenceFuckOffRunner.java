package com.nekolr;

import com.baomidou.dynamic.datasource.toolkit.DynamicDataSourceContextHolder;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author excalibur
 * @date 2021/11/16 9:43
 */
@Component
@Slf4j
public class SequenceFuckOffRunner {

    private static final String CATALOG_CAT = "TABLE_CAT";

    private static final String TABLE_TYPE_TABLE = "TABLE";

    private static final String DEFAULT_SCHEMA = "public";

    private static final String RESULT_SET_TABLE_COLUMN = "TABLE_NAME";

    private static final String RESULT_SET_COLUMN = "COLUMN_NAME";

    private static final Pattern COLUMN_DEFAULT_PATTERN = Pattern.compile("\\'([^\\']*)\\'");

    private static final String SELECT_SEQUENCE_SQL = "SELECT column_name, column_default from information_schema.columns where table_name = ?";


    private DataSource dynamicRoutingDataSource;

    public SequenceFuckOffRunner(DataSource dynamicRoutingDataSource) {
        this.dynamicRoutingDataSource = dynamicRoutingDataSource;
    }

    public void run(String datasource) {
        DynamicDataSourceContextHolder.push(datasource);

        try {
            Connection connection = dynamicRoutingDataSource.getConnection();
            DatabaseMetaData databaseMetaData = connection.getMetaData();
            ResultSet catalogs = databaseMetaData.getCatalogs();

            while (catalogs.next()) {
                String catalog = catalogs.getString(CATALOG_CAT);
                log.info("================== catalog：{} ==================\r\n", catalog);

                // 获取所有的表
                ResultSet tables = databaseMetaData.getTables(catalog, DEFAULT_SCHEMA, "%", new String[]{TABLE_TYPE_TABLE});

                while (tables.next()) {
                    String table = tables.getString(RESULT_SET_TABLE_COLUMN);
                    log.info("================ 开始处理表：{} ================", table);

                    // 获取主键
                    ResultSet pkResultSet = databaseMetaData.getPrimaryKeys(catalog, DEFAULT_SCHEMA, table);

                    int index = 0;
                    for (; pkResultSet.next(); index++) {
                        String pk = pkResultSet.getString(RESULT_SET_COLUMN);
                        log.info("获取到主键名称：{}", pk);

                        // 查询该表使用的序列
                        PreparedStatement sequenceRefStatement = connection.prepareStatement(SELECT_SEQUENCE_SQL);
                        sequenceRefStatement.setString(1, table);
                        ResultSet sequenceResultSet = sequenceRefStatement.executeQuery();
                        while (sequenceResultSet.next()) {
                            String column = sequenceResultSet.getString(1);
                            String columnDefault = sequenceResultSet.getString(2);

                            // 如果该列是主键
                            if (column.equals(pk)) {
                                if (StringUtils.isNotBlank(columnDefault)) {
                                    Matcher matcher = COLUMN_DEFAULT_PATTERN.matcher(columnDefault);
                                    if (matcher.find(0)) {
                                        String sequenceName = matcher.group(0).replaceAll("'", "");

                                        // 获取主键最大值
                                        PreparedStatement maxPkStatement = connection.prepareStatement("SELECT " + pk + " FROM " + table + " ORDER BY " + pk + " desc");
                                        ResultSet maxPkResultSet = maxPkStatement.executeQuery();

                                        while (maxPkResultSet.next()) {
                                            if (maxPkResultSet.isFirst()) {
                                                long maxPk = maxPkResultSet.getLong(1);
                                                log.info("获取主键当前最大值：{}", maxPk);

                                                // 更新序列
                                                long nowValue = maxPk + 1;
                                                PreparedStatement updateSequenceStatement = connection.prepareStatement("select setval('" + sequenceName + "', " + nowValue + ")");
                                                ResultSet rs = updateSequenceStatement.executeQuery();
                                                while (rs.next()) {
                                                    long latestValue = rs.getLong(1);
                                                    if (latestValue == nowValue) {
                                                        log.info("更新序列：{} 当前值为：{} 成功", sequenceName, nowValue);
                                                    } else {
                                                        log.info("更新序列：{} 当前值为：{} 失败", sequenceName, nowValue);
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                                break;
                            }
                        }
                    }

                    if (index == 0) {
                        log.info("该表没有设置主键，跳过");
                    }
                }
            }

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            DynamicDataSourceContextHolder.clear();
        }
    }
}
