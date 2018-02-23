package TrackAction;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

public class DateDaysAgo {
    int period_hours=2;
    DateFormat dateFormatDaily = new SimpleDateFormat("yyyy-MM-dd");
    DateFormat dateFormatHourly = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    Date date = new Date();

    //  print current date in format "2018-01-01"
    public String getToday() throws ParseException {
        return dateFormatDaily.format(date);
    }

    //  print date in format "2018-01-01"
    public String getFormatDate(Date d) throws ParseException {
        return dateFormatDaily.format(d);
    }

    //  print current date in format "2018-01-01 01:01:01"
    public String getTodayHourly() throws ParseException {
        return dateFormatHourly.format(date);
    }

    //  print date in format "2018-01-01 01:01:01"
    public String getFormatHourly(Date d) throws ParseException {
        return dateFormatHourly.format(d);
    }

    // date in format String  minus days
    public String getDaysAgo(String day, int daysago) {
        String newDateStr="";
        try {
            Date date2=date;
            if (day != null) {
                date2 = dateFormatDaily.parse(day);
            }
            GregorianCalendar cal = new GregorianCalendar();
            cal.setTime(date2);
            cal.add(Calendar.DATE, daysago);
            Date newDate = cal.getTime();
            newDateStr = dateFormatDaily.format(newDate);
        } catch (ParseException e) {
            System.out.println("Parse Error="+day);
        }
        return newDateStr;
    }

    // date in format String  minus periods (2 hour chunks)
    public String getHoursAgo(String day, int periods) {
        String newDateStr="";
        try {
            Date date2=date;
            if (day != null) {
                date2 = dateFormatHourly.parse(day);
            }
            GregorianCalendar cal = new GregorianCalendar();
            cal.setTime(date2);
            cal.add(Calendar.HOUR, periods*period_hours);
            Date newDate = cal.getTime();
            newDateStr = dateFormatHourly.format(newDate);
        } catch (ParseException e) {
            System.out.println("Parse Error="+day);
        }
        return newDateStr;
    }

    void printElements(Date d) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(d);
        int year = cal.get(Calendar.YEAR);
        int month = cal.get(Calendar.MONTH);
        int day = cal.get(Calendar.DAY_OF_MONTH);
        int hour = cal.get(Calendar.HOUR_OF_DAY);
        System.out.println(year+" "+month+" " +day+" "+hour);
        cal.set(year,month,day,hour,0,0);
        System.out.println(cal.getTime());
        String news = dateFormatHourly.format(cal.getTime());
        System.out.println(news);
    }
    // Convert partition format tp normal date format
    // 2018-01-06 08:00:00 -> 1808-01-06 00:00:00
    public String dateToPartition(String d) {
        //d="2018-01-06 08:00:00";
        String year = d.substring(2,4)+d.substring(11,13);
        String month_day = d.substring(4,10);
        //result="1808-01-06 00:00:00";
        return year+month_day+" 00:00:00";
    }
    // Convert partition format to normal date format
    // 1808-01-06 00:00:00 -> 2018-01-06 08:00:00
    public Date partitionToDate(String d) {
        //d="1808-01-06 00:00:00";
        System.out.println(d);
        String year = "20"+d.substring(0,2);
        String month_day = d.substring(4,10);
        String hour = d.substring(2,4);
        String d2 = year+month_day+" "+hour+":00:00";
        //result="2018-01-06 08:00:00";
        Date result = null;
        try {
            result = dateFormatHourly.parse(d2);
        } catch (ParseException e) {
            System.out.println("Parse Error="+d);
        }
        return result;
    }

    public String getCurrentPeriod() {
        return getPeriod(new Date());
    }

    // get time dropping minutes and seconds
    public String getPeriod(Date d) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(d);
        System.out.println(dateFormatHourly.format(cal.getTime()));
        int year = cal.get(Calendar.YEAR);
        int month = cal.get(Calendar.MONTH);
        int day = cal.get(Calendar.DAY_OF_MONTH);
        int hour = cal.get(Calendar.HOUR_OF_DAY);
        cal.set(year,month,day,hour,0,0);
        return dateFormatHourly.format(cal.getTime());
    }

    public static void main(String[] args) throws Exception {
        DateDaysAgo ss = new DateDaysAgo();
        String today=ss.getTodayHourly();
        System.out.println("today="+today);
        System.out.println(ss.date);
        String s = ss.getDaysAgo(today,-1);
        System.out.println("days="+s);
        String s2 = ss.getHoursAgo(today,-1);
        System.out.println("hours="+s2);
        ss.printElements(ss.date);
        String sss = ss.dateToPartition("2018-01-06 08:00:00");
        System.out.println("sss="+sss);
        System.out.println(ss.getCurrentPeriod());
        Date ddd = ss.partitionToDate("1808-01-06 00:00:00");
        System.out.println(ss.getFormatHourly(ddd));
    }
}
