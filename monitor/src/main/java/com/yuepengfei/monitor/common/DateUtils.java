package com.suning.flink.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class DateUtils {
	
    private static Logger LOG = LoggerFactory.getLogger(DateUtils.class);

	public static final String DATE_FORMAT = "yyyyMMdd";
	public static final String DATE_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
	public static final String LONG_DATE_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss SSS";
	
	/**
     * 日期格式化样式：yyyyMMddHHmmssSSS
     */
    public static final String yyyyMMddHHmmssSSS = "yyyyMMddHHmmssSSS";
    
    /**
     * 日期格式化样式：yyyyMMddHHmmss
     */
    public static final String yyyyMMddHHmmss = "yyyyMMddHHmmss";
    
    /**
     * 日期格式化样式：yyyy-MM-dd HH:mm:ss
     */
    public static final String yyyyMMddHHmmssFormatS1 = "yyyy-MM-dd HH:mm:ss";
    
    /**
     * 日期格式化样式：yyyy-MM-dd HH:mm:ss SSS
     */
    public static final String yyyyMMddHHmmssFormatS2 = "yyyy-MM-dd HH:mm:ss SSS";
    
    /**
     * 日期格式化样式：yyyyMMdd
     */
    public static final String yyyyMMdd = "yyyyMMdd";
    
    /**
     * 日期格式化样式：yyyyMMddHH
     */
    public static final String yyyyMMddHH = "yyyyMMddHH";
    
    /**
     * 日期格式化样式：yyyy-MM-dd
     */
    public static final String yyyyMMddFormatS1 = "yyyy-MM-dd";
    
    /**
     * 日期格式化样式：yyyy：MM：dd
     */
    public static final String yyyyMMddFormatS2 = "yyyy:MM:dd";
    
    /**
     * 时间格式化样式：HH时mm分ss秒
     */
    public static final String HHmmss1 = "HH时mm分ss秒";
    
    /**
     * 时间格式化样式：HH:mm:ss
     */
    public static final String HHmmss3 = "HH:mm:ss";
    
    /**
     * 时间格式化样式：HHmmss
     */
    public static final String HHmmss2 = "HHmmss";
	
	
	/** 锁对象 */
    private static final byte[] lockObj = new byte[1];
    
    private static final String REG_EXPR_STR15 = "^\\d{15,}$";
    private static final String REG_EXPR_STR14 = "^\\d{14}$";
    private static final String TIME_ZONE = "GMT+8";

    /** 存放不同的日期模板格式的sdf的Map */
    private static Map<String, ThreadLocal<SimpleDateFormat>> sdfMap  = new HashMap<String, ThreadLocal<SimpleDateFormat>>();

    /**
     * 返回一个ThreadLocal的sdf,每个线程只会new一次sdf
     * @param pattern
     * @return
     */
    private static SimpleDateFormat getSdf(final String pattern) {
        ThreadLocal<SimpleDateFormat> tl = sdfMap.get(pattern);
        // 此处的双重判断和同步是为了防止sdfMap这个单例被多次put重复的sdf
        if (tl == null) {
            synchronized (lockObj) {
                tl = sdfMap.get(pattern);
                if (tl == null) {
                    // 只有Map中还没有这个pattern的sdf才会生成新的sdf并放入map
                    // 这里是关键,使用ThreadLocal<SimpleDateFormat>替代原来直接new
                    // SimpleDateFormat
                    tl = new ThreadLocal<SimpleDateFormat>() {
                        @Override
                        protected SimpleDateFormat initialValue() {
                            return new SimpleDateFormat(pattern);
                        }
                    };
                    sdfMap.put(pattern, tl);
                }
            }
        }

        return tl.get();
    }
    
    /**
     * 
     * 功能描述：是用ThreadLocal<SimpleDateFormat>来获取SimpleDateFormat,
     * 这样每个线程只会有一个SimpleDateFormat
     * 输入参数：Date date, String pattern
     * @param 参数说明 返回值: Date date 需要格式化的日期, String pattern 格式化样式
     * @return 返回值  String
     * @throw 异常描述
     * @see 需要参见的其它内容
     */
    public static String format(Date date, String pattern) {
        return getSdf(pattern).format(date);
    }

    /**
     * 
     * 功能描述：是用ThreadLocal<SimpleDateFormat>来获取SimpleDateFormat,
     * 这样每个线程只会有一个SimpleDateFormat 
     * 输入参数：String dateStr, String pattern
     * @param 参数说明 返回值: 类型 <说明>
     * @return 返回值 Date
     * @throw 异常描述
     * @see 需要参见的其它内容
     */
    public static Date parse(String dateStr, String pattern) throws ParseException {
        return getSdf(pattern).parse(dateStr);
    }
	
	public static String getDate(String dateStr) throws ParseException {
		SimpleDateFormat tsdf = new SimpleDateFormat(DATE_TIME_FORMAT);
		SimpleDateFormat sdf = null;
		Date tDate = null;
		if (null != dateStr && dateStr.matches(REG_EXPR_STR15)) {
			sdf = new SimpleDateFormat(yyyyMMddHHmmssSSS);
			tDate = sdf.parse(dateStr);
			return tsdf.format(tDate);
		}
		else if (null != dateStr && dateStr.matches(REG_EXPR_STR14)){
			sdf = new SimpleDateFormat(yyyyMMddHHmmss);
			tDate = sdf.parse(dateStr);
			return tsdf.format(tDate);
		}
		else {
			return null;
		}
	}
	
	/**
	 * 获取当前时间，时间格式为yyyy-MM-dd HH:mm:ss
	 * @return
	 */
	public static String getCurrentTime() {
		TimeZone timeZone = TimeZone.getTimeZone(TIME_ZONE);
    	Calendar calendar = Calendar.getInstance(timeZone);
    	Date nowDate = calendar.getTime();
		SimpleDateFormat sdf = new SimpleDateFormat(DATE_TIME_FORMAT);
		return sdf.format(nowDate);
	}
	
	/**
	 *获取当前日期，日期格式为yyyyMMdd
	 */
	public static String getDate() {
		TimeZone timeZone = TimeZone.getTimeZone(TIME_ZONE);
    	Calendar calendar = Calendar.getInstance(timeZone);
    	Date nowDate = calendar.getTime();
		SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT);
		return sdf.format(nowDate);
	}
	
	/**
	 *获取昨天日期 ，日期格式为yyyyMMdd
	 */
	public static String getYesterday(){
		TimeZone timeZone = TimeZone.getTimeZone(TIME_ZONE);
		Calendar calendar = Calendar.getInstance(timeZone);
    	calendar.add(Calendar.DATE,-1);
    	Date yesterdayDate = calendar.getTime();
    	SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT);
		return sdf.format(yesterdayDate);
	}
	
	/**
	 * 把当前时间按指定的格式转换
	 * @param dataformat
	 * @return
	 */
	public static String getDateTime4Format(String dataformat) {
		TimeZone timeZone = TimeZone.getTimeZone(TIME_ZONE);
    	Calendar calendar = Calendar.getInstance(timeZone);
    	Date nowDate = calendar.getTime();
		SimpleDateFormat sdf = new SimpleDateFormat(dataformat);
		return sdf.format(nowDate);
	}

	
	public static String getTime(long timeLong) {
		Date date = new Date(timeLong);
		SimpleDateFormat sdf = new SimpleDateFormat(DATE_TIME_FORMAT);
		return sdf.format(date);
	}
	
	/** 
     * 将时间戳转换为时间
     */
    public static String stampToDate(String s){
        long lt = Long.parseLong(s);
        Date date = new Date(lt);
        SimpleDateFormat sdf = new SimpleDateFormat(DATE_TIME_FORMAT);
        return sdf.format(date);
    }

	/**
	 * 将时间戳转换为指定的时间格式
	 */
	public static String stampToDate(String s, String timeformat){
		long lt = Long.parseLong(s);
		Date date = new Date(lt);
		SimpleDateFormat sdf = new SimpleDateFormat(timeformat);
		return sdf.format(date);
	}
	
	/**
	 * 将yyyy-MM-dd HH:mm:ss转换成unix时间戳
	 */
	public static long dateToTimestamp(String str) throws ParseException{
		long re_time = 0l;
        SimpleDateFormat sdf = new SimpleDateFormat(DATE_TIME_FORMAT);
        try {
        	Date d = sdf.parse(str);
            re_time = d.getTime();     
        } catch (ParseException e) {
        	LOG.warn("error taking place in parsing date str!!!",e);
        }
        return re_time;
	}


	public static long dateToTimestamp(String str,String format) throws ParseException {
		long re_time = 0l;
		SimpleDateFormat sdf = new SimpleDateFormat(format);
		try {
			Date d = sdf.parse(str);
			re_time = d.getTime();
		} catch (ParseException e) {
			LOG.warn("error taking place in parsing date str!!!",e);
		}
		return re_time;
	}
	
	/**
     * 功能描述: <br>
     * 〈功能详细描述〉
     * @param ms
     * @return
     * @see [相关类/方法](可选)
     * @since [产品/模块版本](可选)
     */
    private static StringBuffer formatTimeByMillSec(Long lms) {
    	Integer ss = 1000;  
    	Integer mi = ss * 60;  
    	Integer hh = mi * 60;  
    	Integer dd = hh * 24;  

    	Long day = lms / dd;  
    	Long hour = (lms - day * dd) / hh;  
    	Long minute = (lms - day * dd - hour * hh) / mi;  
    	Long second = (lms - day * dd - hour * hh - minute * mi) / ss;  
    	Long milliSecond = lms - day * dd - hour * hh - minute * mi - second * ss;  
    	  
    	StringBuffer sb = new StringBuffer();
    	if(day > 0) {  
    	    sb.append(day+"天");  
    	}  
    	if(hour > 0) {  
    	    sb.append(hour+"小时");  
    	}  
    	if(minute > 0) {  
    	    sb.append(minute+"分");  
    	}  
    	if(second > 0) {  
    	    sb.append(second+"秒");  
    	}  
    	if(milliSecond > 0) {  
    	    sb.append(milliSecond+"毫秒");  
    	}
    	return sb;
    }  
    
    /**
     * 
     * 功能描述:负数直接转为正值 返回正值的时间格式化后的数据 <br>
     * 〈功能详细描述〉
     * @param lms
     * @return
     * @see [相关类/方法](可选)
     * @since [产品/模块版本](可选)
     */
    public static String formatTimeAbs(Long lms) { 
        if( null == lms ) return null;
        if(lms<0) {
            lms = Math.abs(lms);
        }
        StringBuffer sb = formatTimeByMillSec(lms);  
        return sb.toString();  
    }
    
    /**
     * 
     * 功能描述：两个日期型相减
     * 输入参数：<按照参数定义顺序> 
     * @param 参数说明
     * 返回值:  类型 <说明> 
     * @return 返回值
     * @throw 异常描述
     * @see 需要参见的其它内容
     */
    public static long subtractionDate(String startDate,String startPattern,String endDate,String endPattern) {
        
        if( startDate != null && endDate != null ) {
            try {
                long t1 = parse(startDate, startPattern).getTime();
                long t2 = parse(endDate, endPattern).getTime();
                return t2 - t1;
            } catch (ParseException e) {
            	LOG.warn("error taking place in parsing date str!!!", e);
            }
        }
        return 0;
    }
	
	/**
     * 
     * 功能描述：两个日期型相减
     * 输入参数：<按照参数定义顺序> 
     * @param 参数说明
     * 返回值:  类型 <说明> 
     * @return 返回值
     * @throw 异常描述
     * @see 需要参见的其它内容
     */
    public static long subtractionDate(String startDate,String endDate,String pattern) {
        
        if( startDate != null && endDate != null ) {
            try {
                long t1 = parse(startDate, pattern).getTime();
                long t2 = parse(endDate, pattern).getTime();
                return t2 - t1;
            } catch (ParseException e) {
            	LOG.warn("error taking place in parsing date str!!!", e);
            }
        }
        
        return 0;
    }

	/**
	 * 判断日期是否是今天，输入标准日期格式: yyyy-MM-dd HH:mm:ss
	 * @param time
	 * @return boolean
	 */
	public static boolean isToday(String time) {
		if (StringUtils.isBlank(time) || "-".equals(time)) {
			return false;
		}
		String directDate = time.replaceAll("-","").split("\\s")[0];
		if(getDate().equals(directDate)){
			return true;
		}else{
			return false;
		}
	}

	/**
	 * 判断时间是否在前后10分钟以内，不在则给当前时间
	 * 输入输出均为标准时间格式：yyyy-MM-dd HH:mm:ss
	 */
	public static String getTenMinTime(String time) {
		String currenttime = getCurrentTime();
		if (StringUtils.isBlank(time) || "-".equals(time)) {
			return currenttime;
		}
		Long etlTime = 0L;
		try {
			etlTime = DateUtils.dateToTimestamp(time);
		} catch (ParseException e) {
			LOG.warn("error taking time in parsing date str!!!", e);
		}
		long currentTimeMillis = System.currentTimeMillis();
		Long minTime = currentTimeMillis - 600000;
		Long maxTime = currentTimeMillis + 600000;
		if (etlTime < minTime || etlTime > maxTime) {
			return currenttime;
		} else {
			return time;
		}
	}
}
