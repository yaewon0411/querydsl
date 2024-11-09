package study.querydsl;

import com.querydsl.core.QueryResults;
import com.querydsl.core.Tuple;
import com.querydsl.jpa.JPAExpressions;
import com.querydsl.jpa.impl.JPAQuery;
import com.querydsl.jpa.impl.JPAQueryFactory;
import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.PersistenceUnit;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.annotation.Transactional;
import study.querydsl.entity.Member;
import study.querydsl.entity.QMember;
import study.querydsl.entity.QTeam;
import study.querydsl.entity.Team;

import java.util.List;

import static com.querydsl.jpa.JPAExpressions.*;
import static org.assertj.core.api.Assertions.*;
import static study.querydsl.entity.QMember.member;
import static study.querydsl.entity.QTeam.team;

@SpringBootTest
@Transactional
public class QuerydslBasicTest {

    @Autowired
    EntityManager em;

    JPAQueryFactory queryFactory;

    @BeforeEach
    public void before(){
        queryFactory = new JPAQueryFactory(em);
        Team teamA = new Team("teamA");
        Team teamB = new Team("teamB");
        em.persist(teamA);
        em.persist(teamB);

        Member member1 = new Member("member1", 10, teamA);
        Member member2 = new Member("member2", 20, teamA);
        Member member3 = new Member("member3", 30, teamB);
        Member member4 = new Member("member4", 40, teamB);

        em.persist(member1);
        em.persist(member2);
        em.persist(member3);
        em.persist(member4);
    }
    @Test
    public void startJPQL(){
        //member1을 찾아라.
        Member findByJpql =
                em.createQuery("select m from Member m where m.username = :username", Member.class)
                .setParameter("username", "member1")
                .getSingleResult();

        assertThat(findByJpql.getUsername()).isEqualTo("member1");
    }
    @Test
    public void startQuerydsl(){
        QMember m = new QMember("m");

        Member findMember = queryFactory
                .select(m)
                .from(m)
                .where(m.username.eq("member1"))
                .fetchOne();

        assertThat(findMember.getUsername()).isEqualTo("member1");
    }
    @Test
    public void search(){
        Member findMember = queryFactory
                .selectFrom(member)
                .where(member.username.eq("member1")
                        .and(member.age.between(10,30)))
                .fetchOne();

        assertThat(findMember.getUsername()).isEqualTo("member1");
    }
    @Test
    public void searchAndParam(){ //and 조건인 경우에 , 로 이어서 쭉 쓸 수 있다

        Member findMember = queryFactory
                .selectFrom(member)
                .where(member.username.eq("member1")
                        ,member.age.eq(10)
                )
                .fetchOne();

        assertThat(findMember.getUsername()).isEqualTo("member1");
    }
    @Test
    public void resultFetch(){

//        //리스트 조회
//        List<Member> fetch = queryFactory
//                .selectFrom(member)
//                .fetch();
//
//        //단건 조회
//        Member fetchOne = queryFactory
//                .selectFrom(member)
//                .fetchOne();
//
//        //첫 데이터만 조회
//        Member fetchFirst = queryFactory
//                .selectFrom(member)
//                .fetchFirst();
//
//        QueryResults<Member> results = queryFactory
//                .selectFrom(member)
//                .fetchResults();

        //results.getTotal();
        //List<Member> content = results.getResults();

        long total = queryFactory
                .selectFrom(member)
                .fetchCount();
    }
    /*

    1. 회원 나이 내림차순
    2. 회원 이름 오름차순
    단 2에서 회원 이름이 없으면 마지막에 출력(nulls last)
     */
    @Test
    public void sort(){
        em.persist(new Member(null, 100));
        em.persist(new Member("member5", 100));
        em.persist(new Member("member6", 100));


        List<Member> result = queryFactory
                .selectFrom(member)
                .where(member.age.eq(100))
                .orderBy(member.age.desc(), member.username.asc().nullsLast())
                .fetch();

        Member member5 = result.get(0);
        Member member6 = result.get(1);
        Member memberNull = result.get(2);
        assertThat(member5.getUsername()).isEqualTo("member5");
        assertThat(member6.getUsername()).isEqualTo("member6");
        assertThat(memberNull.getUsername()).isNull();
    }

    @Test
    public void paging1(){
        List<Member> result = queryFactory
                .selectFrom(member)
                .orderBy(member.username.desc())
                .offset(1) //하나 스킵하고
                .limit(2) //두 개 가져옴
                .fetch();

        for (Member member1 : result) {
            System.out.println("member1 = " + member1);
        }
        
        assertThat(result.size()).isEqualTo(2);
    }

    @Test
    public void paging2(){ //전체 조회수
        QueryResults<Member> queryResults = queryFactory
                .selectFrom(member)
                .orderBy(member.username.desc())
                .offset(1) //하나 스킵하고
                .limit(2) //두 개 가져옴
                .fetchResults(); //카운트 쿼리 하나 나가고 + 컨텐츠 용 쿼리 하나 나감


        assertThat(queryResults.getTotal()).isEqualTo(4);
        assertThat(queryResults.getLimit()).isEqualTo(2);
        assertThat(queryResults.getOffset()).isEqualTo(1);
        assertThat(queryResults.getResults().size()).isEqualTo(2);
    }
    @Test
    public void aggregation(){


        List<Tuple> result = queryFactory
                .select(
                        member.count(),
                        member.age.sum().as("총합"),
                        member.age.avg(),
                        member.age.max(),
                        member.age.min()
                )
                .from(member)
                .fetch();

        Tuple tuple = result.get(0);
        assertThat(tuple.get(member.count())).isEqualTo(4);
        assertThat(tuple.get(member.age.sum())).isEqualTo(100);
        assertThat(tuple.get(member.age.avg())).isEqualTo(25);
        assertThat(tuple.get(member.age.max())).isEqualTo(40);
        assertThat(tuple.get(member.age.min())).isEqualTo(10);
    }
    /*
    팀의 이름과 각 팀의 평균 연령을 구해라
     */
    @Test
    public void group(){

        List<Tuple> result = queryFactory
                .select(team.name, member.age.avg())
                .from(member)
                .join(member.team, team)
                .groupBy(team.name)
                .fetch();
        Tuple teamA = result.get(0);
        Tuple teamB = result.get(1);

        assertThat(teamA.get(team.name)).isEqualTo("teamA");
        assertThat(teamA.get(member.age.avg())).isEqualTo(15);

        assertThat(teamB.get(team.name)).isEqualTo("teamB");
        assertThat(teamB.get(member.age.avg())).isEqualTo(35);
    }
    /*
    팀A에 소속된 모든 회원을 찾아라
     */
    @Test
    public void join(){
        List<Member> result = queryFactory
                .selectFrom(member)
                .leftJoin(member.team, team)
                .where(team.name.eq("teamA"))
                .fetch();

        assertThat(result)
                .extracting("username")
                .containsExactly("member1", "member2");
    }
    /*
    세타 조인
    회원의 이름이 팀 이름과 같은 회원 조회
     */
    @Test
    public void theta_join(){
        em.persist(new Member("teamA"));
        em.persist(new Member("teamB"));

        List<Member> result = queryFactory
                .select(member)
                .from(member, team)
                .where(member.username.eq(team.name))
                .fetch();

        assertThat(result)
                .extracting("username")
                .containsExactly("teamA","teamB");
    }
    /*
    회원과 팀을 조인하면서, 팀 이름이 teamA인 팀만 조인, 회원은 모두 조회
    jpql : select m, t from Member m left join m.team t on t.name = 'teamA'
     */
    @Test
    public void join_on_filtering(){

        List<Tuple> result = queryFactory //select가 여러 가지 타입을 갖고 있기 때문에 tuple로 반환됨
                .select(member, team)
                .from(member)
                .leftJoin(member.team, team).on(team.name.eq("teamA"))
                .fetch();

        for (Tuple tuple : result) {
            System.out.println("tuple = " + tuple);
        }
    }
    /*
    연관관계 없는 엔티티 외부 조인
    회원의 이름이 팀 이름과 같은 대상 외부 조인
     */
    @Test
    public void join_on_no_relation(){
        em.persist(new Member("teamA"));
        em.persist(new Member("teamB"));

        List<Tuple> result = queryFactory
                .select(member, team)
                .from(member)
                .leftJoin(team).on(member.username.eq(team.name)) //보통은 leftJoin(member.team, team)이렇게 해야 하는데 바로 team을 넣기 때문에 on 절에 따른 조건을 적용했을 때 존재하는 경우만 team 데이터를 끌고온다
                .fetch();

        for (Tuple tuple : result) {
            System.out.println("tuple = " + tuple);
        }
    }
    @PersistenceUnit
    EntityManagerFactory emf;

    @Test
    public void fetchJoinNo(){
        em.flush();
        em.clear();
        //페치조인 테스트할 때는 영속성 컨텍스트 한번 디비에 반영하고 날린 뒤 하자

        Member findMember = queryFactory
                .selectFrom(member)
                .where(member.username.eq("member1"))
                .fetchOne();

        boolean loaded = emf.getPersistenceUnitUtil().isLoaded(findMember.getTeam());//로딩된 엔티티인지, 초기화가 안 된 엔티티인지 알려준다
        assertThat(loaded).as("페치 조인 미적용").isFalse();

    }
    @Test
    public void fetchJoinUse(){
        em.flush();
        em.clear();
        //페치조인 테스트할 때는 영속성 컨텍스트 한번 디비에 반영하고 날린 뒤 하자

        Member findMember = queryFactory
                .selectFrom(member)
                .join(member.team, team).fetchJoin()
                .where(member.username.eq("member1"))
                .fetchOne();

        boolean loaded = emf.getPersistenceUnitUtil().isLoaded(findMember.getTeam());//로딩된 엔티티인지, 초기화가 안 된 엔티티인지 알려준다
        assertThat(loaded).as("페치 조인 적용").isTrue();

    }

    //서브쿼리
    // 나이가 가장 많은 회원 조회
    @Test
    public void subQuery(){

        QMember memberSub = new QMember("memberSub");

        List<Member> result = queryFactory
                .selectFrom(member)
                .where(member.age.eq(
                        select(memberSub.age.max())
                                .from(memberSub)
                ))
                .fetch();

        assertThat(result).extracting("age")
                .containsExactly(40);
    }


    //나이가 평균 이상인 회원 조회
    @Test
    public void subQueryGoe(){

        QMember memberSub = new QMember("memberSub");

        List<Member> result = queryFactory
                .selectFrom(member)
                .where(member.age.goe(
                        select(memberSub.age.avg())
                                .from(memberSub)
                ))
                .fetch();

        assertThat(result).extracting("age")
                .containsExactly(30, 40);
    }

    //멤버의 나이가 10살 초과인 회원 조회 (억지성 예제지만 in사용위함)
    @Test
    public void subQueryIn(){

        QMember memberSub = new QMember("memberSub");

        List<Member> result = queryFactory
                .selectFrom(member)
                .where(member.age.in(
                        select(memberSub.age)
                                .from(memberSub)
                                .where(memberSub.age.gt(10)) //10살 초과
                ))
                .fetch();

        assertThat(result).extracting("age")
                .containsExactly(20, 30, 40);
    }


    //select 절에 서브쿼리 사용 예시
    @Test
    public void selectSubQuery(){
        QMember memberSub = new QMember("memberSub");

        List<Tuple> result = queryFactory
                .select(member.username,
                        select(memberSub.age.avg())
                                .from(memberSub))
                .from(member)
                .fetch();

        for (Tuple tuple : result) {
            System.out.println("tuple = " + tuple);
        }

    }
    /*
    * from 절의 서브쿼리 한계
    jpql 서브쿼리의 한계점으로 fmro 절의 서브쿼리는 지원하지 않는다
    * querydsl도 jpql 빌더 역할이므로 당연히 from 절의 서브쿼리 지원 안됨
    * */

    /*
    *from 절의 서브쿼리 해결 방안
    * 1. 서브쿼리를 join으로 변경 -> 대부분의 경우 가능하겠지만 불가능한 상황도 있음
    * 2. 애플리케이션에서 쿼리를 2번 분리해서 실행한다
    * 3. 정 안되면 nativeSQL 사용한다
    *
    * 무엇보다 정말 from 절에서 서브쿼리가 필요한지에 대해서는 고민해야 한다
    * 화면 단에서 렌더링 하는 여러 데이터들을 구성하는 것은 프레젠테이션 계층에서 수행하고
    * db는 기본적으로 데이터를 퍼올리는 역할이라는 것을 인지하자...
    * 물론 쿼리에서 적절한 조건절, 필터링을 수행하는 것은 너무 중요하지만
    * 한 방쿼리로 많은걸 해결하려기보다 적절하게 쿼리를 쪼개는 것도 방안이 될 수 있음을 인지하는게 필요
    *
    * */










}
