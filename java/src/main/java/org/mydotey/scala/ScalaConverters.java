package org.mydotey.scala;

import java.util.function.Supplier;

import scala.Function0;

/**
 * @author koqizhao
 *
 * Dec 10, 2018
 */
public interface ScalaConverters {

    static <R> Function0<R> to(Supplier<R> f) {
        return new Function0<R>() {
            @Override
            public R apply() {
                return f.get();
            }

            @Override
            public byte apply$mcB$sp() {
                return 0;
            }

            @Override
            public char apply$mcC$sp() {
                return 0;
            }

            @Override
            public double apply$mcD$sp() {
                return 0;
            }

            @Override
            public float apply$mcF$sp() {
                return 0;
            }

            @Override
            public int apply$mcI$sp() {
                return 0;
            }

            @Override
            public long apply$mcJ$sp() {
                return 0;
            }

            @Override
            public short apply$mcS$sp() {
                return 0;
            }

            @Override
            public void apply$mcV$sp() {

            }

            @Override
            public boolean apply$mcZ$sp() {
                return false;
            }
        };
    }

}
