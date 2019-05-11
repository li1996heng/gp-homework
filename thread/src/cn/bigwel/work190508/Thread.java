package cn.bigwel.work190508;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

/**
 * @Author: Mica.Li
 * @CreateDate: 2019/5/11 14:46
 * @Version: 1.0
 * @Description:
 */
public class Thread {

    /**
     * 分批计算相似度,取每批最相似的数据,最后汇总得到最终数据
     * 方法有点大, 没来得及拆 :)
     * @param productList
     * @param type
     * @return
     */
    private List<ProductMappingResult> createTheadPool(List<ProductParticiple> venProductParticipleList,ProductParticiple productParticiple,Integer resultNum) {
        // 开始时间
        long start = System.currentTimeMillis();
        // 每500条数据开启一条线程
        int threadSize = 10000;
        // 总数据条数
        int dataSize = venProductParticipleList.size();
        // 线程数
        int threadNum = dataSize / threadSize + 1;
        // 定义标记,过滤threadNum为整数
        boolean special = dataSize % threadSize == 0;
        // 创建一个线程池
        ExecutorService exec = Executors.newFixedThreadPool(threadNum);
        // 定义一个任务集合
        List<Callable<List<ProductMappingResult>>> tasks = new ArrayList<Callable<List<ProductMappingResult>>>();
        Callable<List<ProductMappingResult>> task = null;
        List<ProductParticiple> cutList = null;
        // 确定每条线程的数据
        for (int i = 0; i < threadNum; i++) {
            if (i == threadNum - 1) {
                if (special) {
                    break;
                }
                cutList = venProductParticipleList.subList(threadSize * i, dataSize);
            } else {
                cutList = venProductParticipleList.subList(threadSize * i, threadSize * (i + 1));
            }
            // System.out.println("第" + (i + 1) + "组：" + cutList.toString());
            final List<ProductParticiple> subList = cutList;
            task = new Callable<List<ProductMappingResult>>() {
                @Override
                public List<ProductMappingResult> call() throws Exception {
                    List<Word> disWordList = productParticiple.getWordList();
                    List<ProductMappingResult> oneProductMappingResult = new ArrayList<>();
                    subList.forEach(v -> {
                        ProductMappingResult productMappingResult = new ProductMappingResult();
                        List<Word> venWordList = v.getWordList();

                        // 余弦相似度计算
                        BigDecimal similarRate = CosineSimilarity.cosineSimilarity(disWordList, venWordList);
                        productMappingResult.setDisGoodsId(productParticiple.getId());
                        productMappingResult.setVenGoodsId(v.getId());
                        productMappingResult.setSimilarRate(similarRate);

                        oneProductMappingResult.add(productMappingResult);
                    });
                    // 按照相似度降序排列
                    List<ProductMappingResult> collect = oneProductMappingResult.stream()
                            .sorted(Comparator.comparing(ProductMappingResult::getSimilarRate).reversed()).collect(Collectors.toList());
                    //System.out.println(Thread.currentThread().getName() + "线程：");
                    return collect.subList(0,resultNum);
                }
            };
            // 这里提交的任务容器列表和返回的Future列表存在顺序对应的关系
            tasks.add(task);
        }

        List<Future<List<ProductMappingResult>>> futures = new ArrayList<>();
        try {
            futures = exec.invokeAll(tasks);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        List<ProductMappingResult> productMappingResultList  = new ArrayList<>();
        futures.forEach(f -> {
            Future<List<ProductMappingResult>> listFuture = f;
            try {
                List<ProductMappingResult> mappingResult = listFuture.get();
                productMappingResultList.addAll(mappingResult);
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        });
        // 关闭线程池
        exec.shutdown();
        //System.out.println("线程任务执行结束");
        //System.err.println("执行任务消耗了 ：" + (System.currentTimeMillis() - start) + "毫秒");
        return productMappingResultList;
    }
}
