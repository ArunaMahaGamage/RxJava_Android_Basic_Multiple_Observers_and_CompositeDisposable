package com.aruna.rxjava;

import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import android.util.Log;
import android.widget.TextView;

import io.reactivex.Observable;
import io.reactivex.android.schedulers.AndroidSchedulers;
import io.reactivex.disposables.CompositeDisposable;
import io.reactivex.functions.Function;
import io.reactivex.functions.Predicate;
import io.reactivex.observers.DisposableObserver;
import io.reactivex.schedulers.Schedulers;

public class MainActivity extends AppCompatActivity {

    TextView tv_rx;
    String text = "";

    private static final String TAG = MainActivity.class.getSimpleName();

    private CompositeDisposable compositeDisposable = new CompositeDisposable();

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_main);

        tv_rx = (TextView) findViewById(R.id.tv_rx);

        Observable<String> animalsObservable = getAnimalsObservable();

        DisposableObserver<String> animalsObserver = getAnimalsObserver();

        DisposableObserver<String> animalsObserverAllCaps = getAnimalsAllCapsObserver();

        /**
         * filter() is used to filter out the animal names starting with `b`
         * */
        compositeDisposable.add(
                animalsObservable
                        .subscribeOn(Schedulers.io())
                        .observeOn(AndroidSchedulers.mainThread())
                        .filter(new Predicate<String>() {
                            @Override
                            public boolean test(String s) throws Exception {
                                return s.toLowerCase().startsWith("b");
                            }
                        })
                        .subscribeWith(animalsObserver));

        /**
         * filter() is used to filter out the animal names starting with 'c'
         * map() is used to transform all the characters to UPPER case
         * */

        compositeDisposable.add(
                animalsObservable
                        .subscribeOn(Schedulers.io())
                        .observeOn(AndroidSchedulers.mainThread())
                        .filter(new Predicate<String>() {
                            @Override
                            public boolean test(String s) throws Exception {
                                return s.toLowerCase().startsWith("c");
                            }
                        })
                        .map(new Function<String, String>() {
                            @Override
                            public String apply(String s) throws Exception {
                                return s.toUpperCase();
                            }
                        })
                        .subscribeWith(animalsObserverAllCaps));
    }


    private DisposableObserver<String> getAnimalsObserver() {
        return new DisposableObserver<String>() {

            @Override
            public void onNext(String s) {
                Log.d(TAG, "Name: " + s);

                text += "Name : " + s + "\n";
                tv_rx.setText(text);
            }

            @Override
            public void onError(Throwable e) {
                Log.e(TAG, "onError: " + e.getMessage());

                text += "onError : " + e.getMessage() + "\n";
                tv_rx.setText(text);
            }

            @Override
            public void onComplete() {
                Log.d(TAG, "All items are emitted!");

                text += "All items are emitted!\n";
                tv_rx.setText(text);
            }
        };
    }

    private DisposableObserver<String> getAnimalsAllCapsObserver() {
        return new DisposableObserver<String>() {


            @Override
            public void onNext(String s) {
                Log.d(TAG, "Name: " + s);

                text += "Name : " + s + "\n";
                tv_rx.setText(text);
            }

            @Override
            public void onError(Throwable e) {
                Log.e(TAG, "onError: " + e.getMessage());

                text += "onError : " + e.getMessage() + "\n";
                tv_rx.setText(text);
            }

            @Override
            public void onComplete() {
                Log.d(TAG, "All items are emitted!");

                text += "All items are emitted!\n";
                tv_rx.setText(text);
            }
        };
    }

    private Observable<String> getAnimalsObservable() {
        return Observable.fromArray(
                "Ant", "Ape",
                "Bat", "Bee", "Bear", "Butterfly",
                "Cat", "Crab", "Cod",
                "Dog", "Dove",
                "Fox", "Frog");
    }

    @Override
    protected void onDestroy() {
        super.onDestroy();

        // don't send events once the activity is destroyed
        compositeDisposable.clear();
    }
}
