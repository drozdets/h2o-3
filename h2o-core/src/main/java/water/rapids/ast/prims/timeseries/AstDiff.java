package water.rapids.ast.prims.timeseries;

import water.MRTask;
import water.fvec.Chunk;
import water.fvec.Frame;
import water.fvec.NewChunk;
import water.fvec.Vec;
import water.rapids.Env;
import water.rapids.Val;
import water.rapids.vals.ValFrame;
import water.rapids.ast.AstPrimitive;
import water.rapids.ast.AstRoot;
import water.util.ArrayUtils;

/**
 * Compute a lagged version of a time series, shifting the time base back by a given number of observations.
 */
public class AstDiff extends AstPrimitive {
  @Override
  public String[] args() {
    return new String[]{"ary","lag"};
  }

  @Override
  public String str() {
    return "diff";
  }

  @Override
  public int nargs() {
    return 1 + 2; // (diff x lag)
  }

  @Override
  public Val apply(Env env, Env.StackHelp stk, AstRoot asts[]) {
    Frame fr = stk.track(asts[1].exec(env)).getFrame();
    final double lag = asts[2].exec(env).getNum();
    if (fr.numCols() != 1)
      throw new IllegalArgumentException("Expected a single column for diff. Got: " + fr.numCols() + " columns.");
    if (!fr.anyVec().isNumeric())
      throw new IllegalArgumentException("Expected a numeric column for diff. Got: " + fr.anyVec().get_type_str());
    if(lag > fr.numRows())
      throw new IllegalArgumentException("Lag variable should not be greater than number of rows in the frame. Got: " +
              lag);

    final double[] lastElemPerChk = GetLastElemPerChunkTask.get(fr.anyVec());

    return new ValFrame(new MRTask() {
      @Override
      public void map(Chunk c, NewChunk nc) {
        if (c.cidx() == 0) nc.addNAs((int) lag);
        else nc.addNum(c.atd(0) - lastElemPerChk[c.cidx() - (int) lag]);
        for (int row = (int) lag; row < c._len; ++row)
          nc.addNum(c.atd(row) - c.atd(row - (int) lag));
      }
    }.doAll(fr.types(), fr).outputFrame(fr.names(), fr.domains()));
  }

  private static class GetLastElemPerChunkTask extends MRTask<GetLastElemPerChunkTask> {
    double[] _res;

    GetLastElemPerChunkTask(Vec v) {
      _res = new double[v.espc().length];
    }

    static double[] get(Vec v) {
      GetLastElemPerChunkTask t = new GetLastElemPerChunkTask(v);
      t.doAll(v);
      return t._res;
    }

    @Override
    public void map(Chunk c) {
      _res[c.cidx()] = c.atd(c._len - 1);
    }

    @Override
    public void reduce(GetLastElemPerChunkTask t) {
      ArrayUtils.add(_res, t._res);
    }
  }
}