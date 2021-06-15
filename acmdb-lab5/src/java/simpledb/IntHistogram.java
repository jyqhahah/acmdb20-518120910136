package simpledb;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private class HistogramBar{
        int left, right;
        int tmp, cnt;

        HistogramBar(int left, int right){
            this.left = left;
            this.right = right;
            this.tmp = right - left + 1;
            this.cnt = 0;
        }
    }


    private final int buckets;
    private final int min, max;
    private final int tmp;
    private int tot;
    private final HistogramBar[] HistogramBars;
    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        this.buckets = buckets;
        this.min = min;
        this.max = max;
        int num = max - min + 1;
        if(num % buckets == 0)
            tmp = num / buckets;
        else
            tmp = num / buckets + 1;
        HistogramBars = new HistogramBar[buckets];
        for(int i = 0; i < buckets; ++i){
            int left = min + i * tmp, right;
            if(i == buckets - 1)
                right = max;
            else
                right = left + tmp - 1;
            HistogramBars[i] = new HistogramBar(left, right);
        }
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        tot += 1;
        HistogramBars[(v - min) / tmp].cnt += 1;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
    	// some code goes here
        int index = (v - min) / tmp;
        switch (op){
            case EQUALS:
                if(v < min)
                    return 0;
                if(v > max)
                    return 0;
                return (((double)HistogramBars[index].cnt) / HistogramBars[index].tmp) /tot;
            case GREATER_THAN:
                if(v < min)
                    return 1;
                if(v > max)
                    return 0;
                double sum1 = (((double)(HistogramBars[index].right - v)) /
                        HistogramBars[index].tmp) * HistogramBars[index].cnt;
                for(int i = index + 1; i < buckets; ++i)
                    sum1 += HistogramBars[i].cnt;
                return sum1 / tot;
            case LESS_THAN:
                if(v < min)
                    return 0;
                if(v > max)
                    return 1;
                double sum2 = (((double)(v - HistogramBars[index].left)) /
                        HistogramBars[index].tmp) * HistogramBars[index].cnt;
                for(int i = index - 1; i >= 0; --i)
                    sum2 += HistogramBars[i].cnt;
                return sum2 / tot;
            case NOT_EQUALS:
                return 1 - estimateSelectivity(Predicate.Op.EQUALS, v);
            case LESS_THAN_OR_EQ:
                return 1 - estimateSelectivity(Predicate.Op.GREATER_THAN, v);
            case GREATER_THAN_OR_EQ:
                return 1 - estimateSelectivity(Predicate.Op.LESS_THAN, v);
            default:
                assert false;
        }
        return -1.0;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        StringBuilder stringBuilder = new StringBuilder("Print the IntHistogram: \n");
        stringBuilder.append("left\t\tright\t\tval\n");
        for(int i = 0; i < buckets; ++i){
            stringBuilder.append(HistogramBars[i].left).append("\t\t").
                    append(HistogramBars[i].right).append("\t\t").
                    append(HistogramBars[i].cnt).append("\n");
        }
        return stringBuilder.toString();
    }
}
