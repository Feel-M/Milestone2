package M2;
public class SQLTerm {

    String _strOperator;
    String _strTableName;
    Object _objValue;
    String _strColumnName;

    public SQLTerm() {

    }

    public SQLTerm(String strOperator, String strTableName, Object objValue, String strColumnName)
            throws DBAppException {
        _strTableName = strTableName;
        _objValue = objValue;
        _strColumnName = strColumnName;

        switch (strOperator) {
            case "=":

            case "<":

            case ">":

            case "!=":

            case "<=":

            case ">=":
                _strOperator = strOperator;
                break;

            default:
                throw new DBAppException("Invalid Operator!");

        }

    }
}
