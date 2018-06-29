package com.shuanghe.util;

import javax.servlet.http.HttpServletRequest;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collector;
import java.util.stream.Stream;

/**
 * Description:
 * <p>
 * Date: 2018/06/29
 * Time: 17:24
 *
 * @author yushuanghe
 */
public class CommonUtil {

    private static final Random RANDOM = new Random(System.currentTimeMillis());
    private static final int MIN = 100000;
    private static final int MAX = Integer.MAX_VALUE;

    private static final DecimalFormat amountFormat = new DecimalFormat("#0.00");

    private static final String amountRegx = "^(-)?(([1-9]{1}\\d*)|([0]{1}))(\\.(\\d){1,2})?$";

    public static final Pattern ID_CARD_PATTERN = Pattern.compile("(^\\d{15}$)|(^\\d{17}([0-9]|X|x)$)");

    public static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static String formatDate(Date date) {
        return sdf.format(date);
    }

    public static Integer getTotalPage(Integer limit, Integer total) {
        int totalPage = 0;
        if (limit == -1) {
            totalPage = 1;
        } else if (limit < -1 || limit == 0) {
            return 0;
        } else {
            if (total <= limit) {
                if (total > 0) {
                    totalPage = 1;
                }
            } else {
                int tmp = total % limit;
                if (tmp == 0) {
                    totalPage = total / limit;
                } else {
                    totalPage = (total - tmp) / limit + 1;
                }
            }
        }
        return totalPage;
    }

    public static Integer getOffset(Integer current, Integer limit) {
        if (current <= 0 || limit == -1) {
            current = 1;
        }
        return (current - 1) * limit;
    }

    public static String getPath(Class<?> clazz) {
        String path = "";
        if (System.getProperty("os.name").toLowerCase().indexOf("window") > -1) {
            path = clazz.getResource("/").toString().replace("file:/", "")
                    .replace("%20", " ");
        } else {
            path = clazz.getResource("/").toString().replace("file:", "")
                    .replace("%20", " ");
        }
        return path;
    }

    public static int getIdentifier() {
        return RANDOM.nextInt((MAX - MIN) + 1) + MIN;
    }

    public static int getWordCount(String s) {
        int length = 0;
        for (int i = 0; i < s.length(); i++) {
            int ascii = Character.codePointAt(s, i);
            if (ascii >= 0 && ascii <= 255)
                length++;
            else
                length += 2;
        }
        return length;
    }

    public static String getIpAddr(HttpServletRequest request) {
        String ip = request.getHeader("X-Forwarded-For");
        if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
            ip = request.getHeader("Proxy-Client-IP");
        }
        if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
            ip = request.getHeader("WL-Proxy-Client-IP");
        }
        if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
            ip = request.getHeader("HTTP_CLIENT_IP");
        }
        if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
            ip = request.getHeader("HTTP_X_FORWARDED_FOR");
        }
        if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
            ip = request.getRemoteAddr();
        }
        return ip;
    }

    //获取下一秒
    public static Date getNextSecond(Date date) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.set(Calendar.SECOND, cal.get(Calendar.SECOND) + 1);
        cal.set(Calendar.MILLISECOND, 0);
        return cal.getTime();
    }

    public static boolean isNotPresent(String source, List<String> list) {
        boolean result = true;
        if (list != null && list.size() > 0) {
            for (String str : list) {
                if (source.contains(str)) {
                    result = false;
                    break;
                }
            }
        }
        return result;
    }

    /**
     * object 转 map
     *
     * @param obj
     * @param containSuper 是否包含父类
     * @return
     */
    public static Map<String, String> object2Map(Object obj, boolean containSuper) {
        Map<String, String> map = new HashMap<String, String>();
        try {
            Class<?> clazz = obj.getClass();
            Field[] fields = clazz.getDeclaredFields();
            for (Field field : fields) {
                String key = field.getName();
                Method method = obj.getClass().getDeclaredMethod("get" + capitalise(field.getName()));
                Object value = method.invoke(obj);
                if (value != null) {
                    if (method.getReturnType().getSimpleName().equals("Date")) {
                        map.put(key, date2Str((Date) value));
                    } else {
                        map.put(key, String.valueOf(value));
                    }
                }
            }
            if (containSuper) {
                Class<?> superClass = clazz.getSuperclass();
                if (!"Object".equals(superClass.getSimpleName())) {
                    fields = superClass.getDeclaredFields();
                    for (Field field : fields) {
                        String key = field.getName();
                        Method method = superClass.getDeclaredMethod("get" + capitalise(field.getName()));
                        Object value = method.invoke(obj);
                        if (value != null) {
                            if (method.getReturnType().getSimpleName().equals("Date")) {
                                map.put(key, date2Str((Date) value));
                            } else {
                                map.put(key, String.valueOf(value));
                            }
                        }
                    }
                }
            }
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        }
        return map;
    }

    public static String capitalise(String str) {
        return str.substring(0, 1).toUpperCase() + str.replaceFirst("\\w", "");
    }

    public static String date2Str(Date date) {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return df.format(date);
    }

    public static Double formatAmount(Object amount) {
        return new Double(amountFormat.format(amount));
    }

    public static String parseAmount(Object amount) {
        return amountFormat.format(amount);
    }

    public static boolean isAmount(Object amount) {
        Matcher matcher = Pattern.compile(amountRegx).matcher(String.valueOf(amount));
        return matcher.matches();
    }


    public static Integer calculateDiffByDate(Date former, Date latter) {
        Integer result = null;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Calendar cal = Calendar.getInstance();
        try {
            cal.setTime(sdf.parse(sdf.format(former)));
            long time1 = cal.getTimeInMillis();
            cal.setTime(sdf.parse(sdf.format(latter)));
            long time2 = cal.getTimeInMillis();
            long between_days = (time2 - time1) / (1000 * 3600 * 24);
            result = Integer.parseInt(String.valueOf(between_days));
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return result;
    }

    public static Integer calculateDiffByHour(Date former, Date latter) {
        Integer result = null;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:00:00");
        Calendar cal = Calendar.getInstance();
        try {
            cal.setTime(sdf.parse(sdf.format(former)));
            long time1 = cal.getTimeInMillis();
            cal.setTime(sdf.parse(sdf.format(latter)));
            long time2 = cal.getTimeInMillis();
            long between_hours = (time2 - time1) / (1000 * 3600 * 1);
            result = Integer.parseInt(String.valueOf(between_hours));
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return result;
    }

    /**
     * 根据身份证获取性别 （0：未知 1：男 2：女）
     *
     * @param idcard
     * @return
     */
    public static Integer getSex(String idcard) {
        Integer sex = null;
        if (idcard != null) {
            if (idcard.length() == 15) {
                sex = Integer.valueOf(idcard.charAt(13)) % 2;
            } else if (idcard.length() == 18) {
                sex = Integer.valueOf(idcard.charAt(16)) % 2;
            }
            if (sex != null) {
                if (sex == 0) {
                    sex = 2;
                }
            } else {
                sex = 0;
            }
        } else {
            sex = 0;
        }
        return sex;
    }

    /**
     * 获取订单号
     *
     * @param busiNo 业务号
     * @param num    商品个数
     * @return
     */
    public static String getOrderNo(String busiNo, int num) {
        StringBuilder sb = new StringBuilder();
        sb.append(busiNo).append(getIdentifier()).append(num)
                .append(new SimpleDateFormat("yyMMdd").format(new Date()));
        return sb.toString();
    }

    /**
     * 计算天数
     *
     * @param expire
     * @return
     */
    public static Date getExpireDateByDate(int expire) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date());
        cal.set(Calendar.DAY_OF_MONTH, cal.get(Calendar.DAY_OF_MONTH) + expire);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return cal.getTime();
    }

    public static Date getExpireDateBySecond(int expire) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date());
        cal.set(Calendar.SECOND, cal.get(Calendar.SECOND) + expire);
        cal.set(Calendar.MILLISECOND, 0);
        return cal.getTime();
    }

    /**
     * 计算天数
     *
     * @param expire
     * @return
     */
    public static Date getExpireDateByDate(Date date, int expire) {
        if (date == null) {
            return null;
        }
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.set(Calendar.DAY_OF_MONTH, cal.get(Calendar.DAY_OF_MONTH) + expire);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return cal.getTime();
    }

    /**
     * 计算小时（1小时为一天）
     *
     * @param expire
     * @return
     */
    public static Date getExpireDateByHour(Date date, int expire) {
        if (date == null) {
            return null;
        }
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.set(Calendar.HOUR_OF_DAY, cal.get(Calendar.HOUR_OF_DAY) + expire);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return cal.getTime();
    }

    /**
     * 计算小时（1小时为一天）
     *
     * @param expire
     * @return
     */
    public static Date getExpireDateByHour(int expire) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date());
        cal.set(Calendar.HOUR_OF_DAY, cal.get(Calendar.HOUR_OF_DAY) + expire);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return cal.getTime();
    }

    /**
     * 计算小时
     *
     * @param ttl   小时
     * @param start 起始时间
     * @return
     */
    public static Date getExpireDateByHour(int ttl, long start) {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(start);
        cal.set(Calendar.HOUR_OF_DAY, cal.get(Calendar.HOUR_OF_DAY) + ttl);
        return cal.getTime();
    }

    public static String getUUID() {
        return UUID.randomUUID().toString().replaceAll("-", "");
    }

    public static StringBuilderPlus str4Sign(Object req, List<String> exclude) {
        Map<String, String> paramMap = CommonUtil.object2Map(req, true);
        return str4Sign(paramMap, exclude);
    }

    public static StringBuilderPlus str4Sign(Map<String, String> paramMap, List<String> exclude) {
        TreeMap<String, String> sortedMap = new TreeMap<String, String>();
        sortedMap.putAll(paramMap);
        StringBuilderPlus str4Sign = new StringBuilderPlus();
        for (Map.Entry<String, String> entry : sortedMap.entrySet()) {
            if (exclude == null || (exclude != null && !exclude.contains(entry.getKey()))) {
                if (str4Sign.getStringBuilder().length() > 0) {
                    str4Sign.append("&");
                }
                str4Sign.append(entry.getKey()).append("=").append(entry.getValue());
            }
        }
        return str4Sign;
    }

    // 根据Unicode编码完美的判断中文汉字和符号
    private static boolean isChinese(char c) {
        Character.UnicodeBlock ub = Character.UnicodeBlock.of(c);
        if (ub == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS
                || ub == Character.UnicodeBlock.CJK_COMPATIBILITY_IDEOGRAPHS
                || ub == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS_EXTENSION_A
                || ub == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS_EXTENSION_B
                || ub == Character.UnicodeBlock.CJK_SYMBOLS_AND_PUNCTUATION
                || ub == Character.UnicodeBlock.HALFWIDTH_AND_FULLWIDTH_FORMS
                || ub == Character.UnicodeBlock.GENERAL_PUNCTUATION) {
            return true;
        }
        return false;
    }

    // 完整的判断中文汉字和符号
    public static boolean isChinese(String strName) {
        char[] ch = strName.toCharArray();
        for (int i = 0; i < ch.length; i++) {
            char c = ch[i];
            if (!isChinese(c)) {
                return false;
            }
        }
        return true;
    }

    public static String formatIdCards(String idCards, Collector<CharSequence, ?, String> collector) {
        if (idCards == null) {
            throw new NullPointerException();
        }
        return Stream.of(idCards.split("\\s*,+\\s*")).map(idCard -> {
            if (ID_CARD_PATTERN.matcher(idCard).matches()) {
                return idCard;
            } else {
                throw new RuntimeException();
            }
        }).collect(collector);
    }

    public static double getDecimal(double num, Integer size) {
        if (Double.isNaN(num)) {
            return 0;
        }
        BigDecimal bd = new BigDecimal(num);
        num = bd.setScale(size, BigDecimal.ROUND_HALF_UP).doubleValue();
        return num;
    }

}