package M2;
public class SQLTerm {

    String _strOperator;
    String _strTableName;
    Object _objValue;
    String _strColumnName;

    public SQLTerm() {

    }

    public SQLTerm(String strTableName,String strColumnName,String strOperator,  Object objValue )
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
