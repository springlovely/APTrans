package sqlancer.mysql.oracle.Txn;

import java.util.*;
import java.util.Random;

import sqlancer.Txn_constant;

public class Pattern {
    public static class Operation {
        String type;
        int transactionId;
        int opIndex;

        public Operation(String type, int transactionId, int opIndex) {
            this.type = type;
            this.transactionId = transactionId;
            this.opIndex = opIndex;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) return false;
            Operation op = (Operation) obj;
            return transactionId == op.transactionId &&
                opIndex == op.opIndex &&
                type.equals(op.type);
        }

        @Override
        public int hashCode() {
            return Objects.hash(type, transactionId, opIndex);
        }
    }

    public Txn_constant.Pattern Txn_Pattern;
    private String patternStr;
    public int Txn_var_num;
    public Map<String, Integer> TypeConstrain = new HashMap<>();
    public Map<Operation, String> DataConstrain = new HashMap<>();
    public ArrayList<Integer> Schedule = new ArrayList<>();

    public Pattern() {
        int random = new Random().nextInt(6);
        switch (random) {
            case 0:
            case 1:
            case 2:
            case 3:
                Txn_var_num = 1;
                this.Txn_Pattern = Txn_constant.getPattern(1);
                break;
            case 4:
            case 5:
                Txn_var_num = 2;
                this.Txn_Pattern = Txn_constant.getPattern(2);
                break;
        }

        this.patternStr = Txn_Pattern.toString();
        this.getType();
        this.getData();
        this.getSchedule();
    }

    public void getType() {
        TypeConstrain.clear();
        for (int i = 0; i < patternStr.length(); ) {
            char type = patternStr.charAt(i);
            TypeConstrain.put(String.valueOf(type), TypeConstrain.getOrDefault(String.valueOf(type), 0) + 1);
            i++; 

            while (i < patternStr.length() && Character.isDigit(patternStr.charAt(i))) {
                i++;
            }
            
            while (i < patternStr.length() && Character.isLetter(patternStr.charAt(i))) {
                i++;
            }
        }
    }

    public void getData() {
        DataConstrain.clear();
        Map<Integer, Integer> txnIndex = new HashMap<>();

        for (int i = 0; i < patternStr.length(); ) {
            char type = patternStr.charAt(i++);
            StringBuilder txnBuilder = new StringBuilder();
            while (i < patternStr.length() && Character.isDigit(patternStr.charAt(i))) {
                txnBuilder.append(patternStr.charAt(i++));
            }
            int txnId = Integer.parseInt(txnBuilder.toString());

            StringBuilder dataBuilder = new StringBuilder();
            while (i < patternStr.length() && Character.isLetter(patternStr.charAt(i))) {
                dataBuilder.append(patternStr.charAt(i++));
            }
            String data = dataBuilder.toString();

            int opIdx = txnIndex.getOrDefault(txnId, 1);
            Operation op = new Operation(String.valueOf(type), txnId, opIdx);
            txnIndex.put(txnId, opIdx + 1);

            if (type == 'R' || type == 'W') {
                DataConstrain.put(op, data);
            }
        }
    }

    public void getSchedule() {
        Schedule.clear();

        for (int i = 0; i < patternStr.length(); ) {
            char type = patternStr.charAt(i++);

            StringBuilder txnBuilder = new StringBuilder();
            while (i < patternStr.length() && Character.isDigit(patternStr.charAt(i))) {
                txnBuilder.append(patternStr.charAt(i++));
            }
            int txnId = Integer.parseInt(txnBuilder.toString());

            while (i < patternStr.length() && Character.isLetter(patternStr.charAt(i))) {
                i++;
            }

            Schedule.add(txnId);
        }
    }
}