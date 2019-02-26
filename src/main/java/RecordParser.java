/**
 * @Author: xu.dm
 * @Date: 2019/2/21 11:21
 * @Description:
 */
public class RecordParser {
    private int year;
    private int data;
    private boolean valid;

    public int getYear() {
        return year;
    }

    public int getData() {
        return data;
    }

    public boolean isValid() {
        return valid;
    }

    public void parse(String value){
        String[] sValue = value.split(",");
        try {
            year = Integer.parseInt(sValue[0]);
            data = Integer.parseInt(sValue[1]);
            valid = true;
        }catch (Exception e){
            valid = false;
        }
    }
}
