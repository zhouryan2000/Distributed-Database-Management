package shared;

import app_kvServer.KVServer;
import database.DatabaseManager;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;

public class TableManager {
    private static Logger logger = Logger.getLogger(TableManager.class);
    private DatabaseManager dbManager;
    /*M4*/
    private String queryDelimiter = ";";
    private String tableDelimiter = "@";
    private String columnDelimiter = ",";

    public TableManager(DatabaseManager dbManager) {
        this.dbManager = dbManager;
    }

    public String selectQuery(String tableName, String s) {
        String[] tokens = s.split(queryDelimiter);
        String[] query_columns = tokens[0].split(",");
        String[] conditions = tokens[1].split("&&");
        if (tokens[0].equals("*")) {
            try {
                query_columns = this.dbManager.get(tableName + tableDelimiter + "columns").split(columnDelimiter);
            } catch (Exception e) {
                logger.error(e);
            }
        }
        Map<String, Integer> column2index = new HashMap<>();
        for (int i = 0; i < query_columns.length; i++) {
            column2index.put(query_columns[i], i);
        }

        String result = String.join(",", query_columns) + ";";

        try {
            String[] columns = this.dbManager.get(tableName + tableDelimiter + "columns").split(columnDelimiter);

            ArrayList<Predicate<String>> predicates = new ArrayList<>();

            for (int i = 0; i < conditions.length; i ++) {
                 predicates.add(getPredicate(conditions[i], columns));
            }

//            System.out.println(predicates.size());

            int id = Integer.valueOf(this.dbManager.get(tableName+ tableDelimiter + "id"));

            for (int i = 1; i <= id; i++) {
                String row = this.dbManager.get(tableName + tableDelimiter + i);
                if (row != null && pass_all_predicate(predicates, row)) {
                    String[] values = row.split(",");
                    for (int j = 0; j < values.length; j++) {
                        if (column2index.containsKey(columns[j])) {
                            query_columns[column2index.get(columns[j])] = values[j];
                        }
                    }
                    result += String.join(",", query_columns) + ";";
                }
            }
        } catch (Exception e) {
            logger.error("Table is not defined", e);
            return null;
        }

        if (result.equals("")) return "empty";

        return result;
    }

    public ArrayList<String>  deleteQuery(String tableName, String condition) {
        ArrayList<String> result = new ArrayList<>();

        try {
            String[] columns = this.dbManager.get(tableName + tableDelimiter + "columns").split(columnDelimiter);
            Predicate<String> predicate = getPredicate(condition, columns);

            int id = Integer.valueOf(this.dbManager.get(tableName+ tableDelimiter + "id"));

            for (int i = 1; i <= id; i++) {
                String row = this.dbManager.get(tableName + tableDelimiter + i);
                if (row != null && predicate.test(row)) {
                    System.out.println(row);
                    result.add(tableName+ tableDelimiter + i);
                }
            }
        } catch (Exception e) {
            logger.error("delete error", e);
            return null;
        }

        return result;
    }

    public Map<String, String> updateQuery(String tableName, String s) {
        Map<String, String> result = new HashMap<>();
        String[] tokens = s.split(queryDelimiter);
        String[] update_columns = tokens[0].split(",");
        Map<String, String> column2result = new HashMap<>();
        for (int i = 0; i < update_columns.length; i++) {
            String[] data = update_columns[i].split("=");
            column2result.put(data[0], data[1]);
        }

        try {
            String[] columns = this.dbManager.get(tableName + tableDelimiter + "columns").split(columnDelimiter);
            Predicate<String> predicate = getPredicate(tokens[1], columns);

            int id = Integer.valueOf(this.dbManager.get(tableName+ tableDelimiter + "id"));

            for (int i = 1; i <= id; i++) {
                String row = this.dbManager.get(tableName + tableDelimiter + i);
                if (row != null && predicate.test(row)) {
                    System.out.println(row);
                    String[] values = row.split(",");
                    for (int j = 0; j < values.length; j++) {
                        if (column2result.containsKey(columns[j])) {
                            values[j] = column2result.get(columns[j]);
                        }
                    }
                    result.put(tableName + tableDelimiter + i, String.join(",", values));
                }
            }
        } catch (Exception e) {
            logger.error("Table is not defined", e);
            return null;
        }

        return result;
    }

    private Predicate<String> getPredicate(String condition, String[] columns) {
        if (condition.equals("all")) {
            return s -> true;
        }
        String[] tokens = condition.split(" ");
        int i;
        for (i = 0; i < columns.length; i++) {
            if (tokens[0].equals(columns[i])) break;
        }
        int finalI = i;
        if (tokens[1].equals("=")) {
            return s -> s.split(",")[finalI].equals(tokens[2]);
        }
        if (tokens[1].equals("<")) {
            return s -> s.split(",")[finalI].compareTo(tokens[2]) < 0;
        }
        if (tokens[1].equals(">")) {
            return s -> s.split(",")[finalI].compareTo(tokens[2]) > 0;
        }
        if (tokens[1].equals("<=")) {
            return s -> s.split(",")[finalI].compareTo(tokens[2]) <= 0;
        }
        if (tokens[1].equals(">=")) {
            return s -> s.split(",")[finalI].compareTo(tokens[2]) >= 0;
        }
        if (tokens[1].equals("!=")) {
            return s -> !(s.split(",")[finalI].equals(tokens[2]));
        }

        return s -> true;
    }

    private boolean pass_all_predicate(ArrayList<Predicate<String>> predicates, String row) {
        for (int i = 0; i < predicates.size(); i++) {
            if (!predicates.get(i).test(row)) {
                return false;
            }
        }
        return true;
    }
}
